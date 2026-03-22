from fastapi import FastAPI
import asyncio

from app.core.engine import start_engine
from app.producers.producer import produce_test_event
from app.consumers.consumer import register_consumer
from app.websocket.routes import router as ws_router

app = FastAPI()
app.include_router(ws_router)

@app.on_event("startup")
async def startup():
    register_consumer()
    await start_engine()
    

@app.get("/test")
async def test():
    await produce_test_event()
        
    return {"Status": "event sent"}

@app.get("/load")
async def load_test():
    
    for _ in range(1000):
        await produce_test_event()
        
    return {"status": "sent 1000 events"}