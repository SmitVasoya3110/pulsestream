from collections import defaultdict
from typing import Callable, List, Optional
import time

from app.coordinator.consumer import Consumer, ConsumerState
from app.coordinator.delivery import delivery_tracker


class ConsumerCoordinator:

    def __init__(self):
        # consumer_id -> Consumer
        self.consumers: dict[str, Consumer] = {}

        # topic -> group -> set[consumer_id]
        self.groups = defaultdict(lambda: defaultdict(set))

        self._on_rebalance: Optional[Callable[[str, str, List[str]], None]] = None

    def set_rebalance_handler(
        self,
        handler: Callable[[str, str, List[str]], None],
    ):
        self._on_rebalance = handler

    def register(self, consumer: Consumer):
        self.consumers[consumer.id] = consumer
        consumer.last_heartbeat = time.time()

        print(
            f"[REGISTER] consumer={consumer.id}"
        )

    def get(self, consumer_id: str):
        return self.consumers.get(consumer_id)

    def touch(self, consumer_id: str):
        """6.5.6 — refresh heartbeat timestamp."""
        consumer = self.get(consumer_id)
        if consumer is None:
            return
        consumer.last_heartbeat = time.time()

    def find_stale(self, timeout_sec: float) -> List[str]:
        """6.5.8 — consumers whose heartbeat is older than timeout."""
        now = time.time()
        stale = []

        for consumer_id, consumer in list(self.consumers.items()):
            if consumer.state != ConsumerState.ACTIVE:
                continue
            if (now - consumer.last_heartbeat) >= timeout_sec:
                stale.append(consumer_id)

        return stale

    def subscribe(self, consumer_id: str, topic: str, group: str):
        consumer = self.get(consumer_id)

        if consumer is None:
            raise ValueError(f"Unknown Consumer: {consumer_id}")

        old_group = consumer.groups.get(topic)
        if old_group is not None and old_group != group:
            self.groups[topic][old_group].discard(consumer_id)
            self._rebalance(topic, old_group)

        consumer.topics.add(topic)
        consumer.groups[topic] = group
        consumer.last_heartbeat = time.time()

        self.groups[topic][group].add(consumer_id)

        print(
            f"[SUBSCRIBE] "
            f"consumer={consumer_id} "
            f"topic={topic} "
            f"group={group}"
        )

        self._rebalance(topic, group)

    def unsubscribe(self, consumer_id: str, topic: str):
        consumer = self.get(consumer_id)

        if consumer is None:
            return

        group = consumer.groups.pop(topic, None)
        consumer.topics.discard(topic)
        consumer.last_heartbeat = time.time()

        if group is not None:
            self.groups[topic][group].discard(consumer_id)
            print(
                f"[UNSUBSCRIBE] "
                f"consumer={consumer_id} "
                f"topic={topic}"
            )
            self._rebalance(topic, group)
        else:
            print(
                f"[UNSUBSCRIBE] "
                f"consumer={consumer_id} "
                f"topic={topic}"
            )

    def unregister(self, consumer_id: str):
        consumer = self.consumers.pop(consumer_id, None)

        if consumer is None:
            return

        affected = list(consumer.groups.items())

        for topic, group in affected:
            self.groups[topic][group].discard(consumer_id)

        consumer.topics.clear()
        consumer.groups.clear()
        consumer.state = ConsumerState.DEAD

        delivery_tracker.clear_consumer(consumer_id)

        print(
            f"[UNREGISTER] "
            f"consumer={consumer_id}"
        )

        for topic, group in affected:
            self._rebalance(topic, group)

    def _rebalance(self, topic: str, group: str):
        """6.5.9 — membership changed; notify routing layer."""
        members = sorted(self.groups[topic][group])

        print(
            f"[REBALANCE] "
            f"topic={topic} "
            f"group={group} "
            f"members={members}"
        )

        if self._on_rebalance is not None:
            self._on_rebalance(topic, group, members)


coordinator = ConsumerCoordinator()
