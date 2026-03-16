from app.core.engine import event_bus
from app.core.event import Event

async def price_handler(event: Event):

    print("received event", event.payload)


def register_consumer():
    event_bus.subscribe(
        "price_update",
        price_handler
    )