from app.core.engine import event_bus
from app.core.event import Event
from app.websocket.routes import manager

async def price_handler(event: Event):

    print("received event", event.payload)
    await manager.broadcast(
        {
            'type': event.type,
            "data": event.payload,
            "offset": event.offset
        },
        topic=event.type
        
    )

def register_consumer():
    event_bus.subscribe(
        "price_update",
        price_handler
    )
    
    