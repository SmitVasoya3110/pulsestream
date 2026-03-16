import asyncio

from .event_bus import EventBus

event_bus = EventBus()


async def start_engine():
    asyncio.create_task(event_bus.start())
    
    