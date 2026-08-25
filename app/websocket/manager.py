from collections import defaultdict
from typing import List
import asyncio

from fastapi import WebSocket

from app.websocket.client import ClientConnection
from app.coordinator.consumer import Consumer, ConsumerState
from app.coordinator.coordinator import coordinator
from app.coordinator.delivery import delivery_tracker


class ConnectionManager:

    def __init__(self):
        self.consumers: List[Consumer] = []
        # (topic, group) -> round-robin index
        self.group_index = defaultdict(int)
        coordinator.set_rebalance_handler(self.on_rebalance)

    async def connect(self, websocket: WebSocket):
        await websocket.accept()
        connection = ClientConnection(websocket)
        consumer = Consumer(connection=connection)

        self.consumers.append(consumer)
        connection.task = asyncio.create_task(connection.sender())
        coordinator.register(consumer)

        print(
            f"[CONSUMER CONNECTED] "
            f"id={consumer.id} "
            f"connection={id(consumer.connection)} "
            f"websocket={id(consumer.connection.websocket)}"
        )
        return consumer

    def disconnect(self, consumer: Consumer):
        connection = consumer.connection
        connection.active = False

        if connection.task:
            connection.task.cancel()

        if consumer in self.consumers:
            self.consumers.remove(consumer)

        coordinator.unregister(consumer.id)

    def on_rebalance(self, topic: str, group: str, members: List[str]):
        """6.5.9 — reset routing + reassign orphaned pending deliveries."""
        self.group_index[(topic, group)] = 0

        notice = {
            "type": "rebalance",
            "topic": topic,
            "group": group,
            "members": members,
        }

        for consumer_id in members:
            consumer = coordinator.get(consumer_id)
            if consumer is None or consumer.state != ConsumerState.ACTIVE:
                continue
            try:
                consumer.connection.queue.put_nowait(notice)
            except asyncio.QueueFull:
                print(
                    f"[REBALANCE NOTICE DROPPED] "
                    f"consumer={consumer_id}"
                )

        # Issue 2/3 — move preserved pending to remaining members
        reassigned = delivery_tracker.reassign_orphans(
            topic,
            group,
            members,
        )
        for pending in reassigned:
            consumer = coordinator.get(pending.consumer_id)
            if consumer is None or consumer.state != ConsumerState.ACTIVE:
                continue
            try:
                consumer.connection.queue.put_nowait(pending.message)
            except asyncio.QueueFull:
                print(
                    f"[REASSIGN ENQUEUE FAILED] "
                    f"consumer={pending.consumer_id}"
                )

    async def broadcast(self, message: dict, topic: str):
        group_map = {}

        for consumer in coordinator.consumers.values():
            if consumer.state != ConsumerState.ACTIVE:
                continue

            if topic not in consumer.topics:
                continue

            group = consumer.groups.get(topic, "default")
            group_map.setdefault(group, []).append(consumer)

        for group, consumers in group_map.items():
            if not consumers:
                continue

            key = (topic, group)
            idx = self.group_index[key] % len(consumers)
            selected = consumers[idx]
            self.group_index[key] += 1

            delivery_id = delivery_tracker.track(
                selected.id,
                topic,
                group,
                message,
            )
            if delivery_id is None:
                # Already processed for this group (idempotent skip)
                continue

            payload = delivery_tracker.pending[delivery_id].message

            print(
                f"[ROUTE] "
                f"topic={topic} "
                f"group={group} "
                f"consumer={selected.id} "
                f"delivery={delivery_id}"
            )

            try:
                selected.connection.queue.put_nowait(payload)
            except asyncio.QueueFull:
                selected.state = ConsumerState.DEAD
                selected.connection.active = False
                if selected.connection.task:
                    selected.connection.task.cancel()
                if selected in self.consumers:
                    self.consumers.remove(selected)
                coordinator.unregister(selected.id)
            except Exception as e:
                print(e)
