from dataclasses import dataclass
from typing import Any
import time
import uuid


@dataclass
class Event:
    id: str
    type: str
    payload: Any
    timestamp: float
    
    @staticmethod
    def create(event_type: str, payload: Any):
        return Event(
            id=str(uuid.uuid4()),
            type=event_type,
            payload=payload,
            timestamp=time.time()
        )

