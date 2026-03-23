from fastapi import APIRouter, WebSocket, WebSocketDisconnect
import json

from .manager import ConnectionManager

router = APIRouter()
manager = ConnectionManager()


@router.websocket("/ws")
async def websocket_endpoint(websocket: WebSocket):

    await manager.connect(websocket)

    try:
        while True:
        
            data = await websocket.receive_text()  # keep alive
            message = json.loads(data)

            action = message.get("action")
            topic = message.get("topic")
            
            if action == "subscribe":
                manager.subscribe(websocket, topic)
            elif action == "unsubscribe":
                manager.unsubscribe(websocket, topic)
                
    except WebSocketDisconnect:
        manager.disconnect(websocket)