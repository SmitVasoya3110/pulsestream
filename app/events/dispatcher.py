from app.events.queue import event_queue
from app.streaming. connection_manager import manager

async def event_dispatcher():
    while True:
        event = await event_queue.get()
        
        await manager.broadcast(
            topic=event.topic,
            message=""
        )
        