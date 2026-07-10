from collections import defaultdict
from typing import List
from app.core.event import Event



class EventStore:

    def __init__(self):
        self.store = defaultdict(list)

    def append(self, event:Event):
        self.store[event.type].append(event)

    def get_events(self, topic: str, limit: int = 10):
        return self.store[topic][-limit:]

