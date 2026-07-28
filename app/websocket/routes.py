from fastapi import APIRouter, WebSocket, WebSocketDisconnect
import json

from .manager import ConnectionManager
from app.core.topics import Topic
from app.core.engine import event_bus

router = APIRouter()
manager = ConnectionManager()


@router.websocket("/ws")
async def websocket_endpoint(websocket: WebSocket):

    await manager.connect(websocket)

    try:
        while True:
        
            data = await websocket.receive_text()  # keep alive

            try:
                message = json.loads(data)
            except Exception:
                await websocket.send_json({"error": "invalid_json"})
                continue

            action = message.get("action")
            topic = message.get("topic")
            group = message.get("group", "default")

 
            if action not in ["subscribe", "unsubscribe", "replay"]:
                await websocket.send_json({"error": "invalid_action"})
                continue


            if topic not in Topic._value2member_map_:
                await websocket.send_json({"error": "invalid_topic"})
                continue

            if action == "subscribe":
                manager.subscribe(websocket, topic, group)
                await websocket.send_json({
                    "status": "subscribed",
                    "topic": topic
                })

            elif action == "unsubscribe":
                manager.unsubscribe(websocket, topic)
                await websocket.send_json({
                    "status": "unsubscribed",
                    "topic": topic
                })

            elif action == "replay":
                limit = message.get("limit", 10)
                offset = message.get("offset")

                if offset is not None:
                    events = event_bus.store.get_events_after(topic, offset)
                else:
                    events = event_bus.store.get_events(topic, limit)

                for event in events:
                    await websocket.send_json({
                        "type": event.type,
                        "data": event.payload,
                        "offset": event.offset,
                        "replay": True
                    })

    except WebSocketDisconnect:
        manager.disconnect(websocket)