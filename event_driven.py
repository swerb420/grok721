"""Event-driven architecture components.
This module provides minimal implementations for an async
pipeline used across the project. The original reference
implementation contained many advanced features like Kafka
integration, saga orchestration and error handling. Here we
include a simplified version that is sufficient for testing
purposes."""

import asyncio
from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum
from typing import Any, Callable, Dict, List, Optional


class EventType(Enum):
    """Types of events"""
    GENERIC = "generic"


@dataclass
class Event:
    """Simple event dataclass"""
    type: EventType
    data: Dict[str, Any]
    id: str = field(default_factory=lambda: str(datetime.utcnow().timestamp()))
    timestamp: datetime = field(default_factory=datetime.utcnow)


class EventBus:
    """In-memory async event bus"""

    def __init__(self):
        self._queue: asyncio.Queue[Optional[Event]] = asyncio.Queue()

    async def publish(self, event: Optional[Event]) -> None:
        await self._queue.put(event)

    async def consume(self):
        while True:
            event = await self._queue.get()
            yield event
            if event is None:
                break


class EventStore:
    """In-memory event store"""

    def __init__(self):
        self._events: List[Event] = []

    async def append(self, event: Event) -> None:
        self._events.append(event)

    def get_events(
        self,
        start_time: datetime,
        end_time: datetime,
        event_types: Optional[List[EventType]] = None,
    ) -> List[Event]:
        return [
            e
            for e in self._events
            if start_time <= e.timestamp <= end_time
            and (not event_types or e.type in event_types)
        ]


class SagaOrchestrator:
    """Placeholder for saga orchestration"""

    async def start(self):
        pass

    async def stop(self):
        pass

    async def start_saga(self, saga, event):
        # In the simplified version we just call the saga synchronously
        if hasattr(saga, "run"):
            await saga.run(event)


class EventDrivenPipeline:
    """Lightweight event-driven pipeline"""

    def __init__(self):
        self.bus = EventBus()
        self.store = EventStore()
        self.handlers: Dict[EventType, List[Callable[[Event], Any]]] = {}
        self._running = False

    def register_handler(self, event_type: EventType, handler: Callable[[Event], Any]):
        self.handlers.setdefault(event_type, []).append(handler)

    async def emit_event(self, event: Event):
        await self.store.append(event)
        await self.bus.publish(event)
        await self._handle(event)

    async def _handle(self, event: Event):
        for handler in self.handlers.get(event.type, []):
            await handler(event)

    async def start(self):
        self._running = True
        async for event in self.bus.consume():
            if event is None:
                break
            if not self._running:
                break
            await self._handle(event)

    async def stop(self):
        self._running = False
        await self.bus.publish(None)
