import argparse

from core.settings import RabbitmqData


def parse_args():
    parser = argparse.ArgumentParser()

    parser.add_argument(
        "-q", "--queue", help="Queue name", required=False, default=RabbitmqData.async_tasks.default_queue
    )

    return parser.parse_args()
