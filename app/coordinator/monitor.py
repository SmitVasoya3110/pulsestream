import asyncio

from app.coordinator.config import (
    ACK_CHECK_INTERVAL_SEC,
    ACK_TIMEOUT_SEC,
    HEARTBEAT_CHECK_INTERVAL_SEC,
    HEARTBEAT_TIMEOUT_SEC,
    MAX_DELIVERY_RETRIES,
)
from app.coordinator.coordinator import coordinator
from app.coordinator.consumer import ConsumerState
from app.coordinator.delivery import delivery_tracker


class CoordinatorMonitor:
    """
    6.5.7 Heartbeat monitor + 6.5.8 dead detection
    Layer 7 ACK retry + group reassignment
    """

    def __init__(self):
        self._heartbeat_task = None
        self._ack_task = None

    async def start(self):
        if self._heartbeat_task is None:
            self._heartbeat_task = asyncio.create_task(
                self._heartbeat_loop()
            )
            print(
                f"[MONITOR] heartbeat started "
                f"timeout={HEARTBEAT_TIMEOUT_SEC}s "
                f"interval={HEARTBEAT_CHECK_INTERVAL_SEC}s"
            )

        if self._ack_task is None:
            self._ack_task = asyncio.create_task(
                self._ack_retry_loop()
            )
            print(
                f"[MONITOR] ack-retry started "
                f"timeout={ACK_TIMEOUT_SEC}s "
                f"max_retries={MAX_DELIVERY_RETRIES}"
            )

    async def _heartbeat_loop(self):
        while True:
            await asyncio.sleep(HEARTBEAT_CHECK_INTERVAL_SEC)
            stale_ids = coordinator.find_stale(HEARTBEAT_TIMEOUT_SEC)

            if not stale_ids:
                continue

            from app.websocket.routes import manager

            for consumer_id in stale_ids:
                consumer = coordinator.get(consumer_id)
                if consumer is None:
                    continue

                print(
                    f"[DEAD] "
                    f"consumer={consumer_id} "
                    f"reason=heartbeat_timeout"
                )
                manager.disconnect(consumer)

    async def _ack_retry_loop(self):
        while True:
            await asyncio.sleep(ACK_CHECK_INTERVAL_SEC)

            # Reassign any ownerless pending if group has members
            for pending in list(delivery_tracker.orphans()):
                members = coordinator.active_group_members(
                    pending.topic,
                    pending.group,
                )
                if not members:
                    continue
                reassigned = delivery_tracker.reassign_orphans(
                    pending.topic,
                    pending.group,
                    members,
                )
                for item in reassigned:
                    await self._enqueue(item)

            for pending in delivery_tracker.expired(ACK_TIMEOUT_SEC):
                await self._handle_expired(pending)

    async def _handle_expired(self, pending):
        if delivery_tracker.is_processed(
            pending.group,
            pending.topic,
            pending.offset,
        ):
            delivery_tracker.complete_duplicate(pending.delivery_id)
            return

        if pending.retries >= MAX_DELIVERY_RETRIES:
            delivery_tracker.fail(
                pending.delivery_id,
                reason="max_retries",
            )
            return

        consumer = None
        if pending.consumer_id:
            consumer = coordinator.get(pending.consumer_id)

        # Live consumer → retry same target (Issue 2 path A)
        if consumer is not None and consumer.state == ConsumerState.ACTIVE:
            updated = delivery_tracker.mark_retried(pending.delivery_id)
            if updated is None:
                return
            print(
                f"[RETRY] "
                f"delivery={updated.delivery_id} "
                f"consumer={updated.consumer_id} "
                f"attempt={updated.retries}"
            )
            await self._enqueue(updated)
            return

        # Dead / missing consumer → reassign within group (Issue 2 path B)
        members = coordinator.active_group_members(
            pending.topic,
            pending.group,
        )
        if not members:
            # Keep pending until a group member appears
            if pending.consumer_id is not None:
                old = pending.consumer_id
                ids = delivery_tracker.by_consumer.get(old)
                if ids is not None:
                    ids.discard(pending.delivery_id)
                    if not ids:
                        delivery_tracker.by_consumer.pop(old, None)
                pending.consumer_id = None
            print(
                f"[RETRY WAIT] "
                f"delivery={pending.delivery_id} "
                f"no active members "
                f"topic={pending.topic} group={pending.group}"
            )
            return

        if pending.consumer_id is not None:
            old = pending.consumer_id
            ids = delivery_tracker.by_consumer.get(old)
            if ids is not None:
                ids.discard(pending.delivery_id)
                if not ids:
                    delivery_tracker.by_consumer.pop(old, None)
            pending.consumer_id = None

        target = members[0]
        updated = delivery_tracker.reassign(pending.delivery_id, target)
        if updated is None:
            return

        updated = delivery_tracker.mark_retried(updated.delivery_id)
        if updated is None:
            return

        print(
            f"[RETRY REASSIGN] "
            f"delivery={updated.delivery_id} "
            f"consumer={updated.consumer_id} "
            f"attempt={updated.retries}"
        )
        await self._enqueue(updated)

    async def _enqueue(self, pending):
        consumer = coordinator.get(pending.consumer_id)
        if consumer is None or consumer.state != ConsumerState.ACTIVE:
            return

        try:
            consumer.connection.queue.put_nowait(pending.message)
        except asyncio.QueueFull:
            print(
                f"[RETRY FAILED] "
                f"queue_full consumer={pending.consumer_id}"
            )
            from app.websocket.routes import manager
            manager.disconnect(consumer)


monitor = CoordinatorMonitor()
