from fastapi import WebSocket
from typing import List
import asyncio

from app.websocket.client import ClientConnection


class ConnectionManager:

    def __init__(self):
        self.clients: List[ClientConnection] = []
        

    async def connect(self, websocket: WebSocket):
        await websocket.accept()
        client = ClientConnection(websocket)
        self.clients.append(client)
        
        client.task = asyncio.create_task(client.sender())

    def disconnect(self, websocket: WebSocket):
        for client in self.clients:
            if client.websocket == websocket:
                client.active = False
                self.clients.remove(client)
                break

    async def broadcast(self, message: dict):
        dead_clients = []
        
        for client in self.clients:
            try:
                client.queue.put_nowait(message)
            except asyncio.QueueFull:
                dead_clients.append(client)
                
        for client in dead_clients:
            client.active = False
            self.clients.remove(client)