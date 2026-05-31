import asyncio
import json
import logging
import os

from aiokafka import AIOKafkaConsumer
from dotenv import load_dotenv

load_dotenv()

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger(__name__)

KAFKA_BOOTSTRAP: str = os.environ["KAFKA_BOOTSTRAP"]
RETRY_DELAY = 5


async def consume_water() -> None:
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
                log.info("water  %s", msg.value)
        except Exception as exc:
            log.error("water consumer error (%s) — retrying in %ds", exc, RETRY_DELAY)
        finally:
            await consumer.stop()
        await asyncio.sleep(RETRY_DELAY)


async def consume_bathrooms() -> None:
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
                log.info("bath   %s", msg.value)
        except Exception as exc:
            log.error("bathroom consumer error (%s) — retrying in %ds", exc, RETRY_DELAY)
        finally:
            await consumer.stop()
        await asyncio.sleep(RETRY_DELAY)


async def main() -> None:
    await asyncio.gather(
        consume_water(),
        consume_bathrooms(),
    )


if __name__ == "__main__":
    asyncio.run(main())