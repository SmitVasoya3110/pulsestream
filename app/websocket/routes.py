from fastapi import APIRouter, WebSocket, WebSocketDisconnect
from .manager import ConnectionManager

router = APIRouter()
manager = ConnectionManager()


@router.websocket("/ws")
async def websocket_endpoint(websocket: WebSocket):

    await manager.connect(websocket)

    try:
        while True:
            await websocket.receive_text()  # keep alive

    except WebSocketDisconnect:
        manager.disconnect(websocket)