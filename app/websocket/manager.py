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

    def subscribe(self, websocket:WebSocket, topic:str):
        print("[SUBSCRIBE] ", websocket, topic)
        for client in self.clients:
            if client.websocket == websocket:
                client.topics.add(topic)
                break
    
    def unsubscribe(self, websocket: WebSocket, topic: str):
        for client in self.clients:
            if client.websocket == websocket:
                client.topics.discard(topic)
                break
    
    async def broadcast(self, message: dict, topic: str):
        dead_clients = []
        
        for client in self.clients:
            if topic not in client.topics:
                continue
            try:
                client.queue.put_nowait(message)
            except asyncio.QueueFull:
                dead_clients.append(client)
                
        for client in dead_clients:
            client.active = False
            self.clients.remove(client)