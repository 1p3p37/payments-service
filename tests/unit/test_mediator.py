from collections.abc import AsyncIterator
from dataclasses import dataclass

import pytest
from dishka import AsyncContainer, Provider, Scope, make_async_container, provide

from app.application.mediator import Command, HandlerNotRegistered, Mediator, Query


@dataclass(frozen=True)
class Ping(Command[str]):
    text: str


@dataclass(frozen=True)
class Count(Query[int]):
    pass


@dataclass(frozen=True)
class Unregistered(Command[None]):
    pass


class Counter:
    value = 0


@dataclass
class PingHandler:
    counter: Counter

    async def handle(self, command: Ping) -> str:
        self.counter.value += 1
        return f"pong: {command.text}"


@dataclass
class CountHandler:
    counter: Counter

    async def handle(self, query: Count) -> int:
        return self.counter.value


class DummyProvider(Provider):
    scope = Scope.REQUEST
    counter = provide(Counter)
    ping = provide(PingHandler)
    count = provide(CountHandler)

    @provide
    def mediator(self, container: AsyncContainer) -> Mediator:
        return Mediator(container, {Ping: PingHandler, Count: CountHandler})


@pytest.fixture
async def mediator() -> AsyncIterator[Mediator]:
    container = make_async_container(DummyProvider())
    async with container() as request:
        yield await request.get(Mediator)
    await container.close()


async def test_dispatches_to_registered_handlers(mediator: Mediator):
    assert await mediator.send(Ping("hi")) == "pong: hi"
    # хендлеры одного скоупа делят зависимости (как сессию в приложении)
    assert await mediator.query(Count()) == 1


async def test_unregistered_request(mediator: Mediator):
    with pytest.raises(HandlerNotRegistered, match="Unregistered"):
        await mediator.send(Unregistered())
