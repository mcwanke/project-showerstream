import asyncio
import json
import logging
import os

import websockets
import yaml
from dotenv import load_dotenv
from kafka import KafkaProducer

load_dotenv()

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger(__name__)

HA_URL: str = os.environ["HA_URL"]
HA_TOKEN: str = os.environ["HA_TOKEN"]
KAFKA_BOOTSTRAP: str = os.environ["KAFKA_BOOTSTRAP"]
TOPIC_WATER = "home.water"
TOPIC_BATHROOMS = "home.bathrooms"
RETRY_DELAY = 5


def load_entity_map() -> dict[str, dict]:
    with open("config.yaml") as f:
        config = yaml.safe_load(f)

    entity_map: dict[str, dict] = {}

    wm = config["water_meter"]
    entity_map[wm["flow_rate"]] = {"topic": TOPIC_WATER, "field": "flow_rate_gpm"}
    entity_map[wm["volume"]] = {"topic": TOPIC_WATER, "field": "volume_gallons"}

    for bathroom_id, bathroom in config["bathrooms"].items():
        for section in ("sensors", "baselines", "devices"):
            for entry in bathroom.get(section, []):
                entity_map[entry["entity"]] = {
                    "topic": TOPIC_BATHROOMS,
                    "bathroom_id": bathroom_id,
                    "role": entry["role"],
                }

    return entity_map


def parse_state(state: str) -> bool | float | None:
    if state in ("on", "off"):
        return state == "on"
    try:
        return float(state)
    except ValueError:
        return None


def make_kafka_producer() -> KafkaProducer:
    return KafkaProducer(
        bootstrap_servers=KAFKA_BOOTSTRAP,
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        key_serializer=lambda k: k.encode("utf-8") if k else None,
    )


async def stream(kafka: KafkaProducer, entity_map: dict[str, dict]) -> None:
    async with websockets.connect(HA_URL) as ws:
        msg = json.loads(await ws.recv())
        assert msg["type"] == "auth_required", f"unexpected: {msg}"

        await ws.send(json.dumps({"type": "auth", "access_token": HA_TOKEN}))
        msg = json.loads(await ws.recv())
        if msg["type"] != "auth_ok":
            raise RuntimeError(f"HA auth failed: {msg}")
        log.info("authenticated with Home Assistant")

        await ws.send(json.dumps({"id": 1, "type": "subscribe_events", "event_type": "state_changed"}))
        msg = json.loads(await ws.recv())
        if not msg.get("success"):
            raise RuntimeError(f"subscription failed: {msg}")
        log.info("subscribed to state_changed — watching %d entities", len(entity_map))

        async for raw in ws:
            event = json.loads(raw)
            if event.get("type") != "event":
                continue

            data = event["event"]["data"]
            entity_id: str = data["entity_id"]

            if entity_id not in entity_map:
                continue

            new_state = data.get("new_state")
            if new_state is None:
                continue

            value = parse_state(new_state["state"])
            if value is None:
                continue

            timestamp: str = new_state["last_changed"]
            routing = entity_map[entity_id]

            if routing["topic"] == TOPIC_WATER:
                kafka.send(TOPIC_WATER, value={"timestamp": timestamp, routing["field"]: value})
                log.debug("water  %s=%s", routing["field"], value)
            else:
                kafka.send(
                    TOPIC_BATHROOMS,
                    key=routing["bathroom_id"],
                    value={"timestamp": timestamp, "bathroom_id": routing["bathroom_id"], routing["role"]: value},
                )
                log.debug("bath   %s %s=%s", routing["bathroom_id"], routing["role"], value)


async def main() -> None:
    entity_map = load_entity_map()
    log.info("loaded %d entities from config", len(entity_map))

    while True:
        kafka = None
        try:
            kafka = make_kafka_producer()
            await stream(kafka, entity_map)
        except Exception as exc:
            log.error("disconnected (%s) — retrying in %ds", exc, RETRY_DELAY)
        finally:
            if kafka:
                kafka.close()
        await asyncio.sleep(RETRY_DELAY)


if __name__ == "__main__":
    asyncio.run(main())
