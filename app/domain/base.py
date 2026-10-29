import json
from abc import ABC, abstractmethod
from dataclasses import asdict, dataclass, field
from datetime import UTC, datetime
from typing import Any, ClassVar
from uuid import UUID, uuid4


@dataclass
class BaseEvent:
    event_name: ClassVar[str]
    event_id: UUID = field(default_factory=uuid4, kw_only=True)

    def to_dict(self) -> dict[str, Any]:
        """JSON-сериализуемый payload события. event_id уходит отдельно (outbox.id / message_id)."""
        data = asdict(self)
        data.pop("event_id")
        return json.loads(json.dumps(data, default=str))


@dataclass(eq=False)
class BaseEntity(ABC):
    id: UUID = field(default_factory=uuid4, kw_only=True)
    created_at: datetime = field(default_factory=lambda: datetime.now(UTC), kw_only=True)
    _events: list[BaseEvent] = field(default_factory=list, kw_only=True, repr=False)

    @abstractmethod
    def to_dict(self) -> dict[str, Any]:
        """Состояние сущности для хранения; значения как есть (Decimal, datetime, enum)."""

    def register_event(self, event: BaseEvent) -> None:
        self._events.append(event)

    def pull_events(self) -> list[BaseEvent]:
        events, self._events = self._events, []
        return events

    def __eq__(self, other: object) -> bool:
        if not isinstance(other, BaseEntity):
            return NotImplemented
        return type(self) is type(other) and self.id == other.id

    def __hash__(self) -> int:
        return hash((type(self), self.id))
