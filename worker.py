import asyncio

import orjson
from aio_pika import IncomingMessage

import async_tasks
from async_tasks.apm import apm_client
from async_tasks.args import parse_args
from async_tasks.broker import Broker
from core.apm import apm_transaction
from core.logging import get_logger
from db import database

logger = get_logger("async_tasks.lib.worker")


async def on_message(message: IncomingMessage) -> None:
    body = orjson.loads(message.body)
    logger.info(f"Received message {body} with message_id={body['message_id']}")
    fn = async_tasks.TASKS[body["fn"]]
    with apm_transaction(apm_client, "async_tasks", fn.__name__, raise_exception=False):
        await fn(*body.get("args", []), **body.get("kwargs", {}))
    await message.ack()
    logger.info(f"Task with message_id={body['message_id']} was finish")


async def main(queue: str) -> None:
    with apm_transaction(apm_client, "async_tasks", "service_startup", raise_exception=True):
        logger.info("Running worker thread...")
        channel = await Broker(prefetch_count=10).channel()

        logger.info("Start import tasks...")
        async_tasks.import_tasks()

        queue = await channel.declare_queue(queue)

        logger.info("Connect to database...")
        await database.connect()
        await queue.consume(on_message)


if __name__ == "__main__":
    args = parse_args()
    loop = asyncio.get_event_loop()
    loop.create_task(main(args.queue))
    loop.run_forever()
