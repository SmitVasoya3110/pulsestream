from collections import defaultdict
from typing import List
from app.core.event import Event



class EventStore:

    def __init__(self):
        self.store = defaultdict(list)
        self.offsets = defaultdict(int)

    def append(self, event:Event):
        offset = self.offsets[event.type]
        event.offset = offset

        self.store[event.type] .append(event)
        self.offsets[event.type] += 1

    def get_events(self, topic: str, limit: int = 10):
        return self.store[topic][-limit:]

    def get_events_after(self, topic:str, offset:int):
        return [e for e in self.store[topic] if e.offset > offset]
