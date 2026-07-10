import asyncio
import time
from typing import Callable, Dict, List
from .event import Event
from app.core.event_store import EventStore

class EventBus:
    
    def __init__(self) -> None:
        self.queue = asyncio.Queue()
        self.processed = 0
        self.start_time = time.perf_counter()
        self.subscribers: Dict[str, List[Callable]] = {}
        self.store = EventStore()
        
    async def publish(self, event: Event):
        self.store.append(event)

        start = time.perf_counter()
        event.enqueued_at = start
        await self.queue.put(event)
        
        end = time.perf_counter()
        print(f"[PUBLISH] latency={(end-start)*1000:.3f} ms")
        
            
    def subscribe(self, event_type: str, handler: Callable):
        if event_type not in self.subscribers:
            self.subscribers[event_type] = []
            
        self.subscribers[event_type].append(handler)
        
    async def start(self):
        while True:
            event:Event = await self.queue.get()
            
            now = time.perf_counter()
            queue_wait = (now-event.enqueued_at) * 1000
            print(f"[QUEUE WAIT] {queue_wait:.3f} ms")
            
            handlers = self.subscribers.get(event.type, [])

            await asyncio.gather(
                *(
                    self._execute(handler, event)
                    for handler in handlers
                )
            )
                
                
    async def _execute(self, handler, event):
        start = time.perf_counter()
        
        await handler(event)
        self.processed += 1 
               
        end = time.perf_counter()
        
        total_latency = (end-event.timestamp)*1000
        handler_time = (end-start)*1000
        print(f"[HANDLER] {handler_time:.3f} ms | [E2E] {total_latency:.3f} ms")
        
        elapsed = time.perf_counter() - self.start_time
        if self.processed % 100 == 0:
            print(f"[THROUGHPUT] {self.processed / elapsed:.2f} event/sec")
            
    