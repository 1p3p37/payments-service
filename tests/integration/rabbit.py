import asyncio

from aio_pika.abc import AbstractIncomingMessage
from faststream.rabbit import RabbitBroker, RabbitQueue


async def wait_message(
    broker: RabbitBroker, queue: RabbitQueue, timeout: float = 5.0, *, no_ack: bool = True
) -> AbstractIncomingMessage | None:
    """Забрать одно сообщение из очереди, подождав до `timeout` секунд."""
    declared = await broker.declare_queue(queue)
    loop = asyncio.get_running_loop()
    deadline = loop.time() + timeout
    while True:
        message = await declared.get(no_ack=no_ack, fail=False)
        if message is not None or loop.time() >= deadline:
            return message
        await asyncio.sleep(0.05)
