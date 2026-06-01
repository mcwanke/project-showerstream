import asyncio
import json
import logging
import os
from datetime import datetime

from aiokafka import AIOKafkaConsumer
from dotenv import load_dotenv
from influxdb_client import Point
from influxdb_client.client.influxdb_client_async import InfluxDBClientAsync

load_dotenv()

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger(__name__)

KAFKA_BOOTSTRAP: str = os.environ["KAFKA_BOOTSTRAP"]
INFLUXDB_URL: str = os.environ["INFLUXDB_URL"]
INFLUXDB_TOKEN: str = os.environ["INFLUXDB_TOKEN"]
INFLUXDB_ORG: str = os.environ["INFLUXDB_ORG"]
INFLUXDB_BUCKET: str = os.environ["INFLUXDB_BUCKET"]
WATER_COST_PER_GALLON: float = float(os.getenv("WATER_COST_PER_GALLON", "0.018"))
RETRY_DELAY = 5


async def consume_showers(write_api, bucket: str) -> None:
    while True:
        consumer = AIOKafkaConsumer(
            "home.showers",
            bootstrap_servers=KAFKA_BOOTSTRAP,
            group_id="influxdb-sink",
            value_deserializer=lambda v: json.loads(v),
            auto_offset_reset="earliest",
        )
        try:
            await consumer.start()
            log.info("showers consumer started")
            async for msg in consumer:
                data = msg.value
                try:
                    ts = datetime.fromisoformat(data["ended_at"])
                    cost = round(data["volume_gallons"] * WATER_COST_PER_GALLON, 4)
                    point = (
                        Point("home_showers")
                        .tag("bathroom_id", data["bathroom_id"])
                        .tag("attribution_state", data["attribution_state"])
                        .field("duration_seconds", int(data["duration_seconds"]))
                        .field("volume_gallons", float(data["volume_gallons"]))
                        .field("confidence_score", float(data["confidence_score"]))
                        .field("cost_estimate", cost)
                        .field("session_id", data["session_id"])
                        .time(ts, "s")
                    )
                    for bathroom_id, score in data.get("scores", {}).items():
                        point = point.field(f"score_{bathroom_id}", float(score))
                    await write_api.write(bucket=bucket, record=point)
                    log.info("wrote session %s to influxdb", data["session_id"])
                except Exception as exc:
                    log.error("failed to write session to influxdb: %s", exc)
        except Exception as exc:
            log.error("showers consumer error (%s) — retrying in %ds", exc, RETRY_DELAY)
        finally:
            await consumer.stop()
        await asyncio.sleep(RETRY_DELAY)


async def consume_shower_scores(write_api, bucket: str) -> None:
    while True:
        consumer = AIOKafkaConsumer(
            "home.shower_scores",
            bootstrap_servers=KAFKA_BOOTSTRAP,
            group_id="influxdb-sink",
            value_deserializer=lambda v: json.loads(v),
            auto_offset_reset="earliest",
        )
        try:
            await consumer.start()
            log.info("shower scores consumer started")
            async for msg in consumer:
                data = msg.value
                try:
                    ts = datetime.fromisoformat(data["timestamp"])
                    point = (
                        Point("home_shower_scores")
                        .tag("session_id", data["session_id"])
                        .time(ts, "s")
                    )
                    for bathroom_id, score in data.get("scores", {}).items():
                        point = point.field(f"score_{bathroom_id}", float(score))
                    await write_api.write(bucket=bucket, record=point)
                    log.debug("wrote score snapshot for session %s", data["session_id"])
                except Exception as exc:
                    log.error("failed to write score snapshot to influxdb: %s", exc)
        except Exception as exc:
            log.error("shower scores consumer error (%s) — retrying in %ds", exc, RETRY_DELAY)
        finally:
            await consumer.stop()
        await asyncio.sleep(RETRY_DELAY)


async def consume_bathrooms(write_api, bucket: str) -> None:
    while True:
        consumer = AIOKafkaConsumer(
            "home.bathrooms",
            bootstrap_servers=KAFKA_BOOTSTRAP,
            group_id="influxdb-sink",
            value_deserializer=lambda v: json.loads(v),
            auto_offset_reset="earliest",
        )
        try:
            await consumer.start()
            log.info("bathroom consumer started")
            async for msg in consumer:
                data = msg.value
                try:
                    ts = datetime.fromisoformat(data["timestamp"])
                    bathroom_id = data["bathroom_id"]
                    point = (
                        Point("home_bathrooms")
                        .tag("bathroom_id", bathroom_id)
                        .time(ts, "s")
                    )
                    fields_written = 0
                    for key, val in data.items():
                        if key in ("timestamp", "bathroom_id"):
                            continue
                        if isinstance(val, bool):
                            point = point.field(key, 1.0 if val else 0.0)
                            fields_written += 1
                        elif isinstance(val, (int, float)):
                            point = point.field(key, float(val))
                            fields_written += 1
                    if fields_written > 0:
                        await write_api.write(bucket=bucket, record=point)
                        log.debug("wrote bathroom reading for %s", bathroom_id)
                except Exception as exc:
                    log.error("failed to write bathroom reading to influxdb: %s", exc)
        except Exception as exc:
            log.error("bathroom consumer error (%s) — retrying in %ds", exc, RETRY_DELAY)
        finally:
            await consumer.stop()
        await asyncio.sleep(RETRY_DELAY)


async def main() -> None:
    async with InfluxDBClientAsync(
        url=INFLUXDB_URL, token=INFLUXDB_TOKEN, org=INFLUXDB_ORG
    ) as client:
        write_api = client.write_api()
        await asyncio.gather(
            consume_showers(write_api, INFLUXDB_BUCKET),
            consume_shower_scores(write_api, INFLUXDB_BUCKET),
            consume_bathrooms(write_api, INFLUXDB_BUCKET),
        )


if __name__ == "__main__":
    asyncio.run(main())
