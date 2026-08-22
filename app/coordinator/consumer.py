from dataclasses import dataclass, field
from app.websocket.client import ClientConnection
from enum import Enum
import time
import uuid

class ConsumerState(str, Enum):
    ACTIVE = "ACTIVE"
    PAUSED = "PAUSED"
    DEAD = "DEAD"



@dataclass
class Consumer:

    connection: ClientConnection = None
    topics: set = field(default_factory=set)
    groups: dict = field(default_factory=dict)
    state: ConsumerState = ConsumerState.ACTIVE
    last_heartbeat: float = field(default_factory=time.time)
    id: str = field(default_factory=lambda: str(uuid.uuid4()))
