import uuid

import orjson
from aio_pika import Message

from .broker import Broker
from core.logging import get_logger
from core.settings import (
    IS_ASYNC_MODE_IN_ASYNC_TASKS,
)
from core.utils import default_json

logger = get_logger("async_tasks.actor")


class Actor:
    def __init__(self, fn, queue_name):
        self.fn = fn
        self.queue_name = queue_name

    def __call__(self, *args, **kwargs):
        return self.fn(*args, **kwargs)

    async def send(self, *args, **kwargs):
        if IS_ASYNC_MODE_IN_ASYNC_TASKS:
            async with Broker() as broker:
                message_id = str(uuid.uuid4())
                await broker.default_exchange.publish(
                    Message(
                        body=orjson.dumps(
                            {"message_id": message_id, "fn": self.fn.__name__, "args": args, "kwargs": kwargs},
                            default=default_json,
                            option=orjson.OPT_NON_STR_KEYS,
                        )
                    ),
                    routing_key=self.queue_name,
                )
                logger.info(f"Task with message_id={message_id} was send in queue={self.queue_name}")
        else:
            await self.fn(*args, **kwargs)
