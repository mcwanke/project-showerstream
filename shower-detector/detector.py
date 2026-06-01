import asyncio
import json
import logging
import os
import uuid
from collections import deque
from datetime import datetime, timezone
from typing import Any, Optional

import yaml
from aiokafka import AIOKafkaConsumer, AIOKafkaProducer
from dotenv import load_dotenv

load_dotenv()

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger(__name__)

KAFKA_BOOTSTRAP: str = os.environ["KAFKA_BOOTSTRAP"]
MIN_FLOW_RATE_OPEN: float = float(os.getenv("MIN_FLOW_RATE_OPEN", "0.5"))
MIN_FLOW_RATE_CLOSE: float = float(os.getenv("MIN_FLOW_RATE_CLOSE", "0.1"))
FLOW_ZERO_TIMEOUT_SECONDS: int = int(os.getenv("FLOW_ZERO_TIMEOUT_SECONDS", "15"))
MIN_SHOWER_DURATION_SECONDS: int = int(os.getenv("MIN_SHOWER_DURATION_SECONDS", "180"))
HUMIDITY_WINDOW_SECONDS = 180
CONFIG_PATH = "/app/config.yaml"
RETRY_DELAY = 5

ATTRIBUTION_CONFIRMED_THRESHOLD: float = float(os.getenv("ATTRIBUTION_CONFIRMED_THRESHOLD", "0.6"))
ATTRIBUTION_MIN_THRESHOLD: float = float(os.getenv("ATTRIBUTION_MIN_THRESHOLD", "0.3"))
BATHROOM_PRIORITY_ORDER: list[str] = [
    b.strip() for b in os.getenv("BATHROOM_PRIORITY_ORDER", "").split(",") if b.strip()
]
HUMIDITY_SLOPE_NORM: float = float(os.getenv("HUMIDITY_SLOPE_NORM", "0.05"))
HUMIDITY_DELTA_NORM: float = float(os.getenv("HUMIDITY_DELTA_NORM", "15.0"))
TEMPERATURE_SLOPE_NORM: float = float(os.getenv("TEMPERATURE_SLOPE_NORM", "0.02"))
TEMPERATURE_DELTA_NORM: float = float(os.getenv("TEMPERATURE_DELTA_NORM", "5.0"))
SCORE_EMIT_INTERVAL_SECONDS: int = int(os.getenv("SCORE_EMIT_INTERVAL_SECONDS", "30"))


def load_bathroom_ids() -> list[str]:
    with open(CONFIG_PATH) as f:
        config = yaml.safe_load(f)
    return list(config["bathrooms"].keys())


def parse_timestamp(ts_str: str) -> datetime:
    try:
        return datetime.fromisoformat(ts_str)
    except (ValueError, AttributeError):
        return datetime.now(timezone.utc)


class SessionManager:
    def __init__(self, bathroom_ids: list[str], producer: AIOKafkaProducer) -> None:
        self._producer = producer
        self._bathroom_ids = bathroom_ids

        self._session_id: Optional[str] = None
        self._started_at: Optional[datetime] = None
        self._last_water_ts: Optional[datetime] = None
        self._last_flow_rate: float = 0.0
        self._volume_accumulated: float = 0.0
        self._close_task: Optional[asyncio.Task] = None
        self._score_emit_task: Optional[asyncio.Task] = None

        self._humidity_history: dict[str, deque] = {bid: deque() for bid in bathroom_ids}
        self._temperature_history: dict[str, deque] = {bid: deque() for bid in bathroom_ids}
        self._baselines: dict[str, dict[str, float]] = {bid: {} for bid in bathroom_ids}
        self._device_states: dict[str, dict[str, Any]] = {bid: {} for bid in bathroom_ids}

        # Peak signal tracking — reset each session
        self._humidity_peak_slope: dict[str, float] = {bid: 0.0 for bid in bathroom_ids}
        self._humidity_peak_delta: dict[str, float] = {bid: 0.0 for bid in bathroom_ids}
        self._temperature_peak_slope: dict[str, float] = {bid: 0.0 for bid in bathroom_ids}
        self._temperature_peak_delta: dict[str, float] = {bid: 0.0 for bid in bathroom_ids}
        self._scores: dict[str, float] = {bid: 0.0 for bid in bathroom_ids}

        # Device seen-state — whether each signal was ever True during the session
        self._light_shower_seen: dict[str, bool] = {bid: False for bid in bathroom_ids}
        self._light_room_seen: dict[str, bool] = {bid: False for bid in bathroom_ids}
        self._fan_seen: dict[str, bool] = {bid: False for bid in bathroom_ids}

    @property
    def _session_open(self) -> bool:
        return self._session_id is not None

    def on_water_message(self, msg: dict) -> None:
        if "flow_rate_gpm" not in msg:
            return

        flow_rate: float = msg["flow_rate_gpm"]
        ts = parse_timestamp(msg.get("timestamp", ""))

        if not self._session_open:
            if flow_rate >= MIN_FLOW_RATE_OPEN:
                self._open_session(flow_rate, ts)
        else:
            if flow_rate >= MIN_FLOW_RATE_OPEN:
                if self._close_task and not self._close_task.done():
                    self._close_task.cancel()
                    self._close_task = None
                    log.info("close timer cancelled — flow recovered")
                self._accumulate_volume(flow_rate, ts)
            elif flow_rate < MIN_FLOW_RATE_CLOSE:
                if not self._close_task or self._close_task.done():
                    log.info("flow below %.2f GPM — starting %ds close timer", MIN_FLOW_RATE_CLOSE, FLOW_ZERO_TIMEOUT_SECONDS)
                    self._close_task = asyncio.create_task(self._close_session())
            # flow between MIN_FLOW_RATE_CLOSE and MIN_FLOW_RATE_OPEN: hold, do nothing

    def _open_session(self, flow_rate: float, ts: datetime) -> None:
        self._session_id = str(uuid.uuid4())
        self._started_at = ts
        self._last_water_ts = ts
        self._last_flow_rate = flow_rate
        self._volume_accumulated = 0.0
        self._score_emit_task = asyncio.create_task(self._score_emit_loop())
        log.info("session opened: %s", self._session_id)

    async def _score_emit_loop(self) -> None:
        try:
            while self._session_open:
                await asyncio.sleep(SCORE_EMIT_INTERVAL_SECONDS)
                if not self._session_open:
                    break
                payload = {
                    "session_id": self._session_id,
                    "timestamp": datetime.now(timezone.utc).isoformat(),
                    "scores": {b: round(self._scores[b], 3) for b in self._bathroom_ids},
                }
                try:
                    await self._producer.send("home.shower_scores", value=json.dumps(payload).encode())
                except Exception as exc:
                    log.error("failed to emit score update for session %s: %s", self._session_id, exc)
        except asyncio.CancelledError:
            pass

    def _accumulate_volume(self, flow_rate: float, ts: datetime) -> None:
        if self._last_water_ts is not None:
            elapsed_minutes = (ts - self._last_water_ts).total_seconds() / 60.0
            avg_flow = (self._last_flow_rate + flow_rate) / 2.0
            self._volume_accumulated += avg_flow * elapsed_minutes
        self._last_water_ts = ts
        self._last_flow_rate = flow_rate

    async def _close_session(self) -> None:
        try:
            await asyncio.sleep(FLOW_ZERO_TIMEOUT_SECONDS)
        except asyncio.CancelledError:
            return

        if not self._session_open:
            return

        ended_at = datetime.now(timezone.utc)
        duration_seconds = (ended_at - self._started_at).total_seconds()
        session_id = self._session_id
        started_at = self._started_at
        volume = self._volume_accumulated

        bathroom_id, confidence_score, attribution_state = self._attribute_session()
        all_scores = {b: round(self._scores[b], 3) for b in self._bathroom_ids}
        self._reset_session()

        if duration_seconds < MIN_SHOWER_DURATION_SECONDS:
            log.info("session %s discarded — %.0fs below minimum", session_id, duration_seconds)
            return

        log.info("session %s closing — %.0fs, %.2f gal", session_id, duration_seconds, volume)
        await self._emit_session(
            session_id, started_at, ended_at, duration_seconds, volume,
            bathroom_id, confidence_score, attribution_state, all_scores,
        )

    def _attribute_session(self) -> tuple[str, float, str]:
        for bid in self._bathroom_ids:
            self._scores[bid] = self._compute_score(bid)

        winner = max(self._bathroom_ids, key=lambda b: self._scores[b])
        score = self._scores[winner]

        if score >= ATTRIBUTION_CONFIRMED_THRESHOLD:
            state = "CONFIRMED"
        elif score >= ATTRIBUTION_MIN_THRESHOLD:
            state = "ATTRIBUTED"
        else:
            state = "FALLBACK"
            priority = [b for b in BATHROOM_PRIORITY_ORDER if b in self._scores]
            if priority:
                winner = priority[0]
                score = self._scores[winner]

        log.info(
            "attribution: %s | scores: %s | state: %s",
            winner,
            {b: round(self._scores[b], 3) for b in self._bathroom_ids},
            state,
        )
        return winner, score, state

    def _compute_score(self, bathroom_id: str) -> float:
        h_slope = min(self._humidity_peak_slope[bathroom_id] / HUMIDITY_SLOPE_NORM, 1.0) * 0.30
        h_delta = min(max(self._humidity_peak_delta[bathroom_id], 0.0) / HUMIDITY_DELTA_NORM, 1.0) * 0.25
        t_slope = min(self._temperature_peak_slope[bathroom_id] / TEMPERATURE_SLOPE_NORM, 1.0) * 0.15
        t_delta = min(max(self._temperature_peak_delta[bathroom_id], 0.0) / TEMPERATURE_DELTA_NORM, 1.0) * 0.15
        l_shower = 0.05 if self._light_shower_seen[bathroom_id] else 0.0
        l_room = 0.05 if self._light_room_seen[bathroom_id] else 0.0
        fan = 0.05 if self._fan_seen[bathroom_id] else 0.0
        return h_slope + h_delta + t_slope + t_delta + l_shower + l_room + fan

    def _recompute_score(self, bathroom_id: str) -> None:
        self._scores[bathroom_id] = self._compute_score(bathroom_id)
        log.debug("scores: %s", {b: round(self._scores[b], 3) for b in self._bathroom_ids})

    def _reset_session(self) -> None:
        self._session_id = None
        self._started_at = None
        self._last_water_ts = None
        self._last_flow_rate = 0.0
        self._volume_accumulated = 0.0
        self._close_task = None
        if self._score_emit_task and not self._score_emit_task.done():
            self._score_emit_task.cancel()
        self._score_emit_task = None
        self._humidity_peak_slope = {bid: 0.0 for bid in self._bathroom_ids}
        self._humidity_peak_delta = {bid: 0.0 for bid in self._bathroom_ids}
        self._temperature_peak_slope = {bid: 0.0 for bid in self._bathroom_ids}
        self._temperature_peak_delta = {bid: 0.0 for bid in self._bathroom_ids}
        self._scores = {bid: 0.0 for bid in self._bathroom_ids}
        self._light_shower_seen = {bid: False for bid in self._bathroom_ids}
        self._light_room_seen = {bid: False for bid in self._bathroom_ids}
        self._fan_seen = {bid: False for bid in self._bathroom_ids}

    async def _emit_session(
        self,
        session_id: str,
        started_at: datetime,
        ended_at: datetime,
        duration_seconds: float,
        volume_gallons: float,
        bathroom_id: str,
        confidence_score: float,
        attribution_state: str,
        all_scores: dict[str, float],
    ) -> None:
        payload = {
            "session_id": session_id,
            "bathroom_id": bathroom_id,
            "attribution_state": attribution_state,
            "started_at": started_at.isoformat(),
            "ended_at": ended_at.isoformat(),
            "duration_seconds": int(duration_seconds),
            "volume_gallons": round(volume_gallons, 3),
            "confidence_score": round(confidence_score, 3),
            "scores": all_scores,
            "cost_estimate": None,
        }
        try:
            await self._producer.send("home.showers", value=json.dumps(payload).encode())
            log.info("emitted session %s to home.showers", session_id)
        except Exception as exc:
            log.error("failed to emit session %s: %s", session_id, exc)

    def on_bathroom_message(self, msg: dict) -> None:
        bathroom_id: str = msg.get("bathroom_id", "")
        if bathroom_id not in self._humidity_history:
            return

        ts = parse_timestamp(msg.get("timestamp", ""))
        ts_float = ts.timestamp()
        cutoff = ts_float - HUMIDITY_WINDOW_SECONDS

        if "humidity" in msg:
            new_val: float = msg["humidity"]
            dq = self._humidity_history[bathroom_id]
            if self._session_open and dq:
                lookback = min(3, len(dq))
                prev_ts, prev_val = dq[-lookback]
                elapsed = ts_float - prev_ts
                if elapsed > 0:
                    slope = (new_val - prev_val) / elapsed
                    if slope > self._humidity_peak_slope[bathroom_id]:
                        self._humidity_peak_slope[bathroom_id] = slope
            dq.append((ts_float, new_val))
            while dq and dq[0][0] < cutoff:
                dq.popleft()
            if self._session_open:
                baseline = self._baselines[bathroom_id].get("humidity_avg_12h")
                if baseline is not None:
                    delta = new_val - baseline
                    if delta > self._humidity_peak_delta[bathroom_id]:
                        self._humidity_peak_delta[bathroom_id] = delta

        elif "temperature" in msg:
            new_val = msg["temperature"]
            dq = self._temperature_history[bathroom_id]
            if self._session_open and dq:
                lookback = min(3, len(dq))
                prev_ts, prev_val = dq[-lookback]
                elapsed = ts_float - prev_ts
                if elapsed > 0:
                    slope = (new_val - prev_val) / elapsed
                    if slope > self._temperature_peak_slope[bathroom_id]:
                        self._temperature_peak_slope[bathroom_id] = slope
            dq.append((ts_float, new_val))
            while dq and dq[0][0] < cutoff:
                dq.popleft()
            if self._session_open:
                baseline = self._baselines[bathroom_id].get("temperature_avg_12h")
                if baseline is not None:
                    delta = new_val - baseline
                    if delta > self._temperature_peak_delta[bathroom_id]:
                        self._temperature_peak_delta[bathroom_id] = delta

        elif "humidity_avg_12h" in msg:
            self._baselines[bathroom_id]["humidity_avg_12h"] = msg["humidity_avg_12h"]

        elif "temperature_avg_12h" in msg:
            self._baselines[bathroom_id]["temperature_avg_12h"] = msg["temperature_avg_12h"]

        else:
            for key, val in msg.items():
                if key not in ("timestamp", "bathroom_id"):
                    self._device_states[bathroom_id][key] = val
                    if self._session_open and val is True:
                        if key == "light_shower":
                            self._light_shower_seen[bathroom_id] = True
                        elif key.startswith("light_"):
                            self._light_room_seen[bathroom_id] = True
                        elif key.startswith("fan_"):
                            self._fan_seen[bathroom_id] = True

        if self._session_open:
            self._recompute_score(bathroom_id)


async def consume_water(manager: SessionManager) -> None:
    while True:
        consumer = AIOKafkaConsumer(
            "home.water",
            bootstrap_servers=KAFKA_BOOTSTRAP,
            group_id="shower-detector",
            value_deserializer=lambda v: json.loads(v),
            auto_offset_reset="latest",
        )
        try:
            await consumer.start()
            log.info("water consumer started")
            async for msg in consumer:
                manager.on_water_message(msg.value)
        except Exception as exc:
            log.error("water consumer error (%s) — retrying in %ds", exc, RETRY_DELAY)
        finally:
            await consumer.stop()
        await asyncio.sleep(RETRY_DELAY)


async def consume_bathrooms(manager: SessionManager) -> None:
    while True:
        consumer = AIOKafkaConsumer(
            "home.bathrooms",
            bootstrap_servers=KAFKA_BOOTSTRAP,
            group_id="shower-detector",
            value_deserializer=lambda v: json.loads(v),
            auto_offset_reset="latest",
        )
        try:
            await consumer.start()
            log.info("bathroom consumer started")
            async for msg in consumer:
                manager.on_bathroom_message(msg.value)
        except Exception as exc:
            log.error("bathroom consumer error (%s) — retrying in %ds", exc, RETRY_DELAY)
        finally:
            await consumer.stop()
        await asyncio.sleep(RETRY_DELAY)


async def main() -> None:
    bathroom_ids = load_bathroom_ids()
    log.info("loaded bathrooms: %s", bathroom_ids)

    producer = AIOKafkaProducer(bootstrap_servers=KAFKA_BOOTSTRAP)
    await producer.start()

    try:
        manager = SessionManager(bathroom_ids, producer)
        await asyncio.gather(
            consume_water(manager),
            consume_bathrooms(manager),
        )
    finally:
        await producer.stop()


if __name__ == "__main__":
    asyncio.run(main())
