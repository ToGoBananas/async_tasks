import asyncio

from aio_pika.robust_connection import connect
from aio_pika.robust_connection import connect_robust

from core.settings import RabbitmqData


class Broker:
    def __init__(
        self,
        ssl: bool = False,
        host: str = RabbitmqData.async_tasks.host,
        login: str = RabbitmqData.async_tasks.login,
        password: str = RabbitmqData.async_tasks.password,
        virtualhost: str = RabbitmqData.async_tasks.virtualhost,
        port: int = 5672,
        prefetch_count=None,
        connection_type=connect,
    ):
        self._ssl = ssl
        self._host = host
        self._login = login
        self._channel = None
        self._connection = None
        self._password = password
        self._virtualhost = virtualhost
        self._port = port
        self.prefetch_count = prefetch_count
        self.connection_type = connection_type

    async def connection(self):
        async with asyncio.Lock():
            if self._connection is None:
                self._connection = await self.connection_type(
                    ssl=self._ssl,
                    host=self._host,
                    login=self._login,
                    password=self._password,
                    virtualhost=self._virtualhost,
                    port=self._port,
                    loop=asyncio.get_running_loop(),
                )
            return self._connection

    async def get_channel(self):
        channel = await (await self.connection()).channel()
        if self.prefetch_count is not None:
            await channel.set_qos(prefetch_count=self.prefetch_count)
        return channel

    async def channel(self):
        async with asyncio.Lock():
            if self._channel is None:
                self._channel = await self.get_channel()

            return self._channel

    async def __aenter__(self):
        return await self.channel()

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        await self._channel.close()
        await self._connection.close()
