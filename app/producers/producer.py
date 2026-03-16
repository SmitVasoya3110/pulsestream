from app.core.engine import event_bus
from app.core.event import Event

async def produce_test_event():
    event = Event.create(
        "price_update",
        {"symbol": "AAPL", "price": 187}
    )
    
    await event_bus.publish(event)