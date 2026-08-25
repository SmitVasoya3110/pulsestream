import asyncio

from .event_bus import EventBus

event_bus = EventBus()
event_bus_started = False


async def start_engine():
    """Start the event-bus dispatcher once (guard against duplicate loops)."""
    global event_bus_started

    if event_bus_started:
        return

    event_bus_started = True
    asyncio.create_task(event_bus.start())
