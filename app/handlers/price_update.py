"""
Internal EventBus handlers.

These are NOT WebSocket / coordinator Consumers.
They subscribe to the in-process EventBus and bridge events
into the delivery plane (ConnectionManager.broadcast).
"""

from app.core.engine import event_bus
from app.core.event import Event
from app.websocket.routes import manager


async def handle_price_update(event: Event):
    """Forward a price_update event to subscribed WebSocket consumers."""
    print("received event", event.payload)
    await manager.broadcast(
        {
            "type": event.type,
            "data": event.payload,
            "offset": event.offset,
            "event_id": event.id,
        },
        topic=event.type,
    )


def register_event_handlers():
    """Wire internal EventBus handlers at application startup."""
    event_bus.subscribe(
        "price_update",
        handle_price_update,
    )
