# async_tasks

A library inside the project for working with asynchronous tasks.

**Quickstart**

Make sure you've got RabbitMQ running, then create a new file called tasks.py in your module:

```python
import async_tasks


@async_tasks.actor
async def add(x, y):
    print(x, y)
```

In one terminal, run your workers:

```commandline
python worker.py -q queue_name
```

Next, call your tasks:
```python
import asyncio
from your_path import *


async def main():
    await add.send(x=1, y=2)

if __name__ == '__main__':
    loop = asyncio.get_event_loop()
    loop.run_until_complete(main())
```