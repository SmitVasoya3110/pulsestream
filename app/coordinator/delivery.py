"""
Layer 7 — reliable delivery (ACK + retry + reassignment).

Delivery guarantee: **at-least-once**.
Duplicates are possible if a consumer processes a message and crashes
before ACK, or if ACK is lost and the message is retried/reassigned.
Exactly-once is NOT provided; use event_id / processed-store for
idempotent handling.
"""

from __future__ import annotations

import time
import uuid
from dataclasses import dataclass, field
from typing import Dict, List, Optional, Set, Tuple


DELIVERY_GUARANTEE = "at-least-once"


@dataclass
class DeliveryMetrics:
    deliveries_sent: int = 0
    acks: int = 0
    ack_rejected: int = 0
    retries: int = 0
    ack_timeouts: int = 0
    failed: int = 0
    reassignments: int = 0
    consumer_failures_during_delivery: int = 0
    duplicates_suppressed: int = 0
    total_ack_latency_ms: float = 0.0
    ack_latency_samples: int = 0

    @property
    def avg_ack_latency_ms(self) -> float:
        if self.ack_latency_samples == 0:
            return 0.0
        return self.total_ack_latency_ms / self.ack_latency_samples

    def snapshot(self) -> dict:
        return {
            "guarantee": DELIVERY_GUARANTEE,
            "deliveries_sent": self.deliveries_sent,
            "acks": self.acks,
            "ack_rejected": self.ack_rejected,
            "retries": self.retries,
            "ack_timeouts": self.ack_timeouts,
            "failed": self.failed,
            "reassignments": self.reassignments,
            "consumer_failures_during_delivery": (
                self.consumer_failures_during_delivery
            ),
            "duplicates_suppressed": self.duplicates_suppressed,
            "pending": None,  # filled by tracker
            "avg_ack_latency_ms": round(self.avg_ack_latency_ms, 3),
        }


@dataclass
class PendingDelivery:
    delivery_id: str
    topic: str
    group: str
    message: dict
    sent_at: float
    created_at: float
    # Current owner — only this consumer may ACK (Section 8)
    consumer_id: Optional[str] = None
    retries: int = 0
    event_id: Optional[str] = None
    offset: Optional[int] = None


@dataclass
class FailedDelivery:
    delivery_id: str
    topic: str
    group: str
    message: dict
    reason: str
    retries: int
    failed_at: float
    last_consumer_id: Optional[str] = None
    event_id: Optional[str] = None
    offset: Optional[int] = None


class DeliveryTracker:
    """
    Tracks unacked deliveries for at-least-once delivery.

    Pending deliveries survive consumer failure and can be reassigned
    to another active member of the same consumer group.
    """

    def __init__(self) -> None:
        self.pending: Dict[str, PendingDelivery] = {}
        self.by_consumer: Dict[str, Set[str]] = {}
        self.failed: Dict[str, FailedDelivery] = {}
        # Idempotency: (group, topic, offset) already ACKed
        self.processed: Set[Tuple[str, str, int]] = set()
        # delivery_ids that completed successfully (late ACK handling)
        self.completed: Set[str] = set()
        self.metrics = DeliveryMetrics()

    def _processed_key(
        self,
        group: str,
        topic: str,
        offset: Optional[int],
    ) -> Optional[Tuple[str, str, int]]:
        if offset is None:
            return None
        return (group, topic, offset)

    def is_processed(self, group: str, topic: str, offset: Optional[int]) -> bool:
        key = self._processed_key(group, topic, offset)
        return key is not None and key in self.processed

    def track(
        self,
        consumer_id: str,
        topic: str,
        group: str,
        message: dict,
    ) -> Optional[str]:
        offset = message.get("offset")
        event_id = message.get("event_id")

        # Idempotency: do not re-deliver an already-ACKed offset for this group
        if self.is_processed(group, topic, offset):
            self.metrics.duplicates_suppressed += 1
            print(
                f"[IDEMPOTENT SKIP] "
                f"topic={topic} group={group} offset={offset}"
            )
            return None

        delivery_id = message.get("delivery_id") or str(uuid.uuid4())
        payload = dict(message)
        payload["delivery_id"] = delivery_id
        payload["require_ack"] = True
        payload["guarantee"] = DELIVERY_GUARANTEE

        now = time.time()
        pending = PendingDelivery(
            delivery_id=delivery_id,
            consumer_id=consumer_id,
            topic=topic,
            group=group,
            message=payload,
            sent_at=now,
            created_at=now,
            event_id=event_id,
            offset=offset if isinstance(offset, int) else None,
        )
        self.pending[delivery_id] = pending
        self.by_consumer.setdefault(consumer_id, set()).add(delivery_id)
        self.metrics.deliveries_sent += 1
        return delivery_id

    def ack(
        self,
        delivery_id: str,
        consumer_id: Optional[str] = None,
    ) -> str:
        """
        Acknowledge a delivery.

        Ownership rule (Section 8):
            ACK accepted only if sender == current delivery owner.

        Other outcomes:
        - already_acked / failed_delivery / unknown_delivery
        - not_owner — wrong consumer or orphaned (no current owner)
        - missing_consumer — caller omitted consumer_id
        """
        if delivery_id in self.completed:
            return "already_acked"

        if delivery_id in self.failed:
            return "failed_delivery"

        pending = self.pending.get(delivery_id)
        if pending is None:
            return "unknown_delivery"

        if consumer_id is None:
            return "missing_consumer"

        # Only the current owner may ACK (after reassignment that is B, not A)
        if pending.consumer_id is None or consumer_id != pending.consumer_id:
            self.metrics.ack_rejected += 1
            print(
                f"[ACK REJECTED] "
                f"delivery={delivery_id} "
                f"sender={consumer_id} "
                f"owner={pending.consumer_id}"
            )
            return "not_owner"

        ack_latency_ms = (time.time() - pending.sent_at) * 1000
        self.metrics.total_ack_latency_ms += ack_latency_ms
        self.metrics.ack_latency_samples += 1
        self.metrics.acks += 1

        key = self._processed_key(pending.group, pending.topic, pending.offset)
        if key is not None:
            self.processed.add(key)

        self._remove(pending)
        self.completed.add(delivery_id)

        print(
            f"[ACK] "
            f"delivery={delivery_id} "
            f"consumer={consumer_id} "
            f"topic={pending.topic} "
            f"group={pending.group} "
            f"offset={pending.offset} "
            f"ack_latency_ms={ack_latency_ms:.3f}"
        )
        return "acked"

    def complete_duplicate(self, delivery_id: str) -> bool:
        """
        Drop a pending delivery that is already in the processed set
        (idempotent cleanup on retry path — no ownership required).
        """
        pending = self.pending.get(delivery_id)
        if pending is None:
            return False
        if not self.is_processed(pending.group, pending.topic, pending.offset):
            return False

        self.metrics.duplicates_suppressed += 1
        self._remove(pending)
        self.completed.add(delivery_id)
        print(
            f"[IDEMPOTENT COMPLETE] "
            f"delivery={delivery_id} "
            f"topic={pending.topic} "
            f"group={pending.group} "
            f"offset={pending.offset}"
        )
        return True

    def release_consumer(self, consumer_id: str) -> List[PendingDelivery]:
        """
        Issue 1/5 — keep pending deliveries when a consumer dies.
        Clears ownership so they can be reassigned to another group member.
        """
        ids = self.by_consumer.pop(consumer_id, set())
        released: List[PendingDelivery] = []

        for delivery_id in ids:
            pending = self.pending.get(delivery_id)
            if pending is None:
                continue
            pending.consumer_id = None
            # Force retry/reassign promptly
            pending.sent_at = 0.0
            released.append(pending)

        if released:
            self.metrics.consumer_failures_during_delivery += 1
            print(
                f"[PENDING PRESERVED] "
                f"consumer={consumer_id} "
                f"count={len(released)}"
            )

        return released

    def reassign(
        self,
        delivery_id: str,
        new_consumer_id: str,
    ) -> Optional[PendingDelivery]:
        pending = self.pending.get(delivery_id)
        if pending is None:
            return None

        old = pending.consumer_id
        if old and old in self.by_consumer:
            self.by_consumer[old].discard(delivery_id)
            if not self.by_consumer[old]:
                self.by_consumer.pop(old, None)

        pending.consumer_id = new_consumer_id
        pending.sent_at = time.time()
        self.by_consumer.setdefault(new_consumer_id, set()).add(delivery_id)
        self.metrics.reassignments += 1

        print(
            f"[REASSIGN] "
            f"delivery={delivery_id} "
            f"from={old} "
            f"to={new_consumer_id} "
            f"topic={pending.topic} "
            f"group={pending.group}"
        )
        return pending

    def reassign_orphans(
        self,
        topic: str,
        group: str,
        member_ids: List[str],
    ) -> List[PendingDelivery]:
        """Assign ownerless pending deliveries for topic/group to members."""
        if not member_ids:
            return []

        reassigned: List[PendingDelivery] = []
        orphans = [
            p for p in self.pending.values()
            if p.topic == topic
            and p.group == group
            and p.consumer_id is None
        ]

        for i, pending in enumerate(orphans):
            if self.is_processed(pending.group, pending.topic, pending.offset):
                self.metrics.duplicates_suppressed += 1
                self._remove(pending)
                self.completed.add(pending.delivery_id)
                continue

            target = member_ids[i % len(member_ids)]
            updated = self.reassign(pending.delivery_id, target)
            if updated is not None:
                reassigned.append(updated)

        return reassigned

    def fail(self, delivery_id: str, reason: str = "max_retries") -> bool:
        """Issue 6 — explicit failure policy: move to dead-letter store."""
        pending = self.pending.get(delivery_id)
        if pending is None:
            return False

        failed = FailedDelivery(
            delivery_id=pending.delivery_id,
            topic=pending.topic,
            group=pending.group,
            message=pending.message,
            reason=reason,
            retries=pending.retries,
            failed_at=time.time(),
            last_consumer_id=pending.consumer_id,
            event_id=pending.event_id,
            offset=pending.offset,
        )
        self.failed[delivery_id] = failed
        self._remove(pending)
        self.metrics.failed += 1
        self.metrics.ack_timeouts += 1

        print(
            f"[FAILED DELIVERY] "
            f"delivery={delivery_id} "
            f"reason={reason} "
            f"topic={pending.topic} "
            f"group={pending.group} "
            f"offset={pending.offset} "
            f"retries={pending.retries}"
        )
        return True

    def mark_retried(self, delivery_id: str) -> Optional[PendingDelivery]:
        pending = self.pending.get(delivery_id)
        if pending is None:
            return None
        pending.retries += 1
        pending.sent_at = time.time()
        self.metrics.retries += 1
        return pending

    def expired(self, timeout_sec: float) -> List[PendingDelivery]:
        now = time.time()
        return [
            p for p in self.pending.values()
            if (now - p.sent_at) >= timeout_sec
        ]

    def orphans(self) -> List[PendingDelivery]:
        return [p for p in self.pending.values() if p.consumer_id is None]

    def _remove(self, pending: PendingDelivery) -> None:
        self.pending.pop(pending.delivery_id, None)
        if pending.consumer_id:
            ids = self.by_consumer.get(pending.consumer_id)
            if ids is not None:
                ids.discard(pending.delivery_id)
                if not ids:
                    self.by_consumer.pop(pending.consumer_id, None)

    def metrics_snapshot(self) -> dict:
        snap = self.metrics.snapshot()
        snap["pending"] = len(self.pending)
        snap["failed_store"] = len(self.failed)
        snap["processed"] = len(self.processed)
        return snap

    def reset(self) -> None:
        """Test helper — clear all delivery state."""
        self.pending.clear()
        self.by_consumer.clear()
        self.failed.clear()
        self.processed.clear()
        self.completed.clear()
        self.metrics = DeliveryMetrics()



delivery_tracker = DeliveryTracker()
