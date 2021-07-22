import os
from importlib import import_module

from async_tasks.actor import Actor
from core.settings import RabbitmqData

TASKS = dict()


def actor(fn=None, actor_class=Actor, queue_name=RabbitmqData.async_tasks.default_queue):
    name = fn.__name__
    TASKS[name] = fn

    def wrapper(fn):
        return actor_class(fn, queue_name)

    return wrapper(fn)


def import_tasks():
    home_dir = os.path.dirname(os.path.dirname(__file__))
    for root, dirs, files in os.walk(home_dir):
        for file in files:
            if file.endswith(".py") and not file.startswith("env"):
                path = os.path.relpath(os.path.join(root, file), start=home_dir).replace("/", ".")
                if (
                    "test" not in path
                    and "fixture" not in path
                    and "scrapy_feed" not in path
                    and path != "core.betradar.dataclasses.py"
                    and "api.openapi" not in path
                ):
                    import_module(f"{path[:-3]}")


__all__ = [
    # Actors
    "Actor",
    "actor",
]

__version__ = "0.1"
