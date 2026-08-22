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
    Layer 7 ACK retry loop
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

            # Lazy import avoids circular import with routes/manager
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
            expired = delivery_tracker.expired(ACK_TIMEOUT_SEC)

            for pending in expired:
                consumer = coordinator.get(pending.consumer_id)

                if (
                    consumer is None
                    or consumer.state != ConsumerState.ACTIVE
                ):
                    delivery_tracker.drop(pending.delivery_id)
                    continue

                if pending.retries >= MAX_DELIVERY_RETRIES:
                    print(
                        f"[ACK TIMEOUT] "
                        f"delivery={pending.delivery_id} "
                        f"consumer={pending.consumer_id} "
                        f"retries={pending.retries} "
                        f"— dropping"
                    )
                    delivery_tracker.drop(pending.delivery_id)
                    continue

                updated = delivery_tracker.mark_retried(
                    pending.delivery_id
                )
                if updated is None:
                    continue

                print(
                    f"[RETRY] "
                    f"delivery={updated.delivery_id} "
                    f"consumer={updated.consumer_id} "
                    f"attempt={updated.retries}"
                )

                try:
                    consumer.connection.queue.put_nowait(
                        updated.message
                    )
                except asyncio.QueueFull:
                    print(
                        f"[RETRY FAILED] "
                        f"queue_full consumer={updated.consumer_id}"
                    )
                    from app.websocket.routes import manager
                    manager.disconnect(consumer)


monitor = CoordinatorMonitor()
