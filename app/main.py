from fastapi import FastAPI

from app.core.engine import start_engine
from app.producers.producer import produce_test_event
from app.consumers.consumer import register_consumer
from app.websocket.routes import router as ws_router
from app.coordinator.monitor import monitor

app = FastAPI()
app.include_router(ws_router)


@app.on_event("startup")
async def startup():
    register_consumer()
    await start_engine()
    await monitor.start()


@app.get("/test")
async def test():
    await produce_test_event()
    return {"Status": "event sent"}


@app.get("/load")
async def load_test():
    for _ in range(1000):
        await produce_test_event()
    return {"status": "sent 1000 events"}


@app.get("/metrics/delivery")
async def delivery_metrics():
    from app.coordinator.delivery import delivery_tracker
    return delivery_tracker.metrics_snapshot()


@app.get("/metrics/failed-deliveries")
async def failed_deliveries():
    from app.coordinator.delivery import delivery_tracker
    return {
        "count": len(delivery_tracker.failed),
        "items": [
            {
                "delivery_id": f.delivery_id,
                "topic": f.topic,
                "group": f.group,
                "offset": f.offset,
                "reason": f.reason,
                "retries": f.retries,
                "event_id": f.event_id,
            }
            for f in delivery_tracker.failed.values()
        ],
    }
