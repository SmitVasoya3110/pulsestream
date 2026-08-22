from __future__ import annotations

import time
import uuid
from dataclasses import dataclass
from typing import Dict, Optional


@dataclass
class PendingDelivery:
    delivery_id: str
    consumer_id: str
    topic: str
    message: dict
    sent_at: float
    retries: int = 0


class DeliveryTracker:
    """Layer 7 — track unacked deliveries and support retry."""

    def __init__(self) -> None:
        self.pending: Dict[str, PendingDelivery] = {}
        # consumer_id -> set of delivery_ids
        self.by_consumer: Dict[str, set] = {}

    def track(self, consumer_id: str, topic: str, message: dict) -> str:
        delivery_id = message.get("delivery_id") or str(uuid.uuid4())
        payload = dict(message)
        payload["delivery_id"] = delivery_id
        payload["require_ack"] = True

        pending = PendingDelivery(
            delivery_id=delivery_id,
            consumer_id=consumer_id,
            topic=topic,
            message=payload,
            sent_at=time.time(),
        )
        self.pending[delivery_id] = pending
        self.by_consumer.setdefault(consumer_id, set()).add(delivery_id)
        return delivery_id

    def ack(self, delivery_id: str, consumer_id: Optional[str] = None) -> bool:
        pending = self.pending.get(delivery_id)
        if pending is None:
            return False

        if consumer_id is not None and pending.consumer_id != consumer_id:
            return False

        self._remove(pending)

        print(
            f"[ACK] "
            f"delivery={delivery_id} "
            f"consumer={pending.consumer_id} "
            f"topic={pending.topic} "
            f"offset={pending.message.get('offset')}"
        )
        return True

    def drop(self, delivery_id: str) -> bool:
        pending = self.pending.pop(delivery_id, None)
        if pending is None:
            return False

        ids = self.by_consumer.get(pending.consumer_id)
        if ids is not None:
            ids.discard(delivery_id)
            if not ids:
                self.by_consumer.pop(pending.consumer_id, None)
        return True

    def _remove(self, pending: PendingDelivery) -> None:
        self.pending.pop(pending.delivery_id, None)
        ids = self.by_consumer.get(pending.consumer_id)
        if ids is not None:
            ids.discard(pending.delivery_id)
            if not ids:
                self.by_consumer.pop(pending.consumer_id, None)

    def clear_consumer(self, consumer_id: str) -> None:
        ids = self.by_consumer.pop(consumer_id, set())
        for delivery_id in ids:
            self.pending.pop(delivery_id, None)

    def expired(self, timeout_sec: float) -> list[PendingDelivery]:
        now = time.time()
        return [
            p for p in self.pending.values()
            if (now - p.sent_at) >= timeout_sec
        ]

    def mark_retried(self, delivery_id: str) -> Optional[PendingDelivery]:
        pending = self.pending.get(delivery_id)
        if pending is None:
            return None
        pending.retries += 1
        pending.sent_at = time.time()
        return pending


delivery_tracker = DeliveryTracker()
