from fastapi import APIRouter, WebSocket
import json 

from app.streaming.connection_manager import manager

router = APIRouter()

@router.websocket("/ws")
async def websocket_endpoint(websocket:WebSocket):
    await manager.connect(websocket)
    
    try:
        while True:
            data = json.loads(await websocket.receive_text())
            
            if data["action"] == "subscribe":
                manager.subscribe(websocket, data["topic"])
                
    except Exception:
        manager.disconnect(websocket)
            