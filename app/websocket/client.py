import asyncio

from fastapi import WebSocket


class ClientConnection:
    def __init__(self, websocket:WebSocket) -> None:
        self.websocket = websocket
        self.queue = asyncio.Queue(maxsize=100)
        self.task = None
        self.active = True
        self.topics = set()
        self.groups = {}
    
    async def sender(self):
        try:
            while self.active:
                message = await self.queue.get()
                await self.websocket.send_json(message)
                
        except Exception:
            self.active = False
            