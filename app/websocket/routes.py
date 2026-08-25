from fastapi import APIRouter, WebSocket, WebSocketDisconnect
import json
import time

from .manager import ConnectionManager
from app.core.topics import Topic
from app.core.engine import event_bus
from app.coordinator.coordinator import coordinator
from app.coordinator.delivery import delivery_tracker


router = APIRouter()
manager = ConnectionManager()


@router.websocket("/ws")
async def websocket_endpoint(websocket: WebSocket):

    consumer = await manager.connect(websocket)

    try:
        while True:

            data = await websocket.receive_text()
            coordinator.touch(consumer.id)

            try:
                message = json.loads(data)
            except Exception:
                await websocket.send_json({"error": "invalid_json"})
                continue

            action = message.get("action")

            # 6.5.6 — explicit heartbeat (any message also touches)
            if action == "heartbeat":
                await websocket.send_json({
                    "status": "ok",
                    "action": "heartbeat",
                    "consumer_id": consumer.id,
                })
                continue

            # Layer 7 — acknowledge a delivery (late ACK allowed)
            if action == "ack":
                delivery_id = message.get("delivery_id")
                if not delivery_id:
                    await websocket.send_json({
                        "error": "missing_delivery_id"
                    })
                    continue

                status = delivery_tracker.ack(
                    delivery_id,
                    consumer_id=consumer.id,
                )
                await websocket.send_json({
                    "status": status,
                    "delivery_id": delivery_id,
                })
                continue

            topic = message.get("topic")
            group = message.get("group", "default")

            if action not in ["subscribe", "unsubscribe", "replay"]:
                await websocket.send_json({"error": "invalid_action"})
                continue

            if topic not in Topic._value2member_map_:
                await websocket.send_json({"error": "invalid_topic"})
                continue

            if action == "subscribe":
                print(
                    f"[SUBSCRIBE REQUEST] "
                    f"consumer={consumer.id} "
                    f"topic={topic} "
                    f"group={group}"
                )

                coordinator.subscribe(consumer.id, topic, group)
                await websocket.send_json({
                    "status": "subscribed",
                    "topic": topic,
                    "group": group,
                })

            elif action == "unsubscribe":
                coordinator.unsubscribe(
                    consumer.id,
                    topic,
                )
                await websocket.send_json({
                    "status": "unsubscribed",
                    "topic": topic,
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
                        "replay": True,
                    })

    except WebSocketDisconnect:
        manager.disconnect(consumer)
