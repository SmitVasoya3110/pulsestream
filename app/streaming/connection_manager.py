from fastapi import WebSocket

class ConnectionManager:
    def __init__(self) -> None:
        self.connections: dict[WebSocket, set[str]] = {}
        
    async def connect(self, websocket: WebSocket):
        await websocket.accept()
        self.connections[websocket] = set()
        
    def disconnect(self, websocket: WebSocket):
        if websocket in self.connections:
            del self.connections[websocket]
            
    def subscribe(self, websocket:WebSocket, topic: str):
        self.connections[websocket].add(topic)
        
    async def broadcast(self, topic: str,  message: str):
        for ws, topics in self.connections.items():
            if topic in topics:
                await ws.send_text(message)

manager = ConnectionManager()