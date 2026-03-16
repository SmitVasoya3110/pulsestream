from fastapi import FastAPI
import asyncio

from app.core.engine import start_engine
from app.producers.producer import produce_test_event
from app.consumers.consumer import register_consumer


app = FastAPI()

@app.on_event("startup")
async def startup():
    register_consumer()
    await start_engine()
    

@app.get("/test")
async def test():
    await produce_test_event()
        
    return {"Status": "event sent"}