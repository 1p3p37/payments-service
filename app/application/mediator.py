from collections.abc import Mapping
from typing import Any

from dishka import AsyncContainer


class Command[R]:
    """Маркер команды (меняет состояние); R — тип результата."""


class Query[R]:
    """Маркер запроса (только чтение); R — тип результата."""


class HandlerNotRegistered(Exception):
    def __init__(self, request_type: type) -> None:
        self.request_type = request_type
        super().__init__(f"No handler registered for {request_type.__name__}")


class Mediator:
    """Минимальный реестр «тип запроса → тип хендлера».

    Хендлер берётся из контейнера текущего скоупа (запрос/сообщение), поэтому у всех
    хендлеров одного скоупа общая сессия. Транзакцией управляет вызывающий код (`async
    with uow:`), а не медиатор.

    Куда расти при необходимости: регистрация декораторами (`@mediator.register`) с
    автосканом модулей application/, несколько хендлеров на событие, pipeline behaviors
    (логирование, метрики, ретраи). Сейчас хендлеров пять — явный словарь нагляднее.
    """

    def __init__(self, container: AsyncContainer, handlers: Mapping[type, type]) -> None:
        self._container = container
        self._handlers = handlers

    async def send[R](self, command: Command[R]) -> R:
        return await self._dispatch(command)

    async def query[R](self, query: Query[R]) -> R:
        return await self._dispatch(query)

    async def _dispatch(self, request: Any) -> Any:
        try:
            handler_type = self._handlers[type(request)]
        except KeyError:
            raise HandlerNotRegistered(type(request)) from None
        handler = await self._container.get(handler_type)
        return await handler.handle(request)
