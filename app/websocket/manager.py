from collections import defaultdict

from fastapi import WebSocket
from typing import List
import asyncio

from app.core.topics import Topic
from app.websocket.client import ClientConnection


class ConnectionManager:

    def __init__(self):
        self.clients: List[ClientConnection] = []
        self.group_index = defaultdict(int)

        

    async def connect(self, websocket: WebSocket):
        await websocket.accept()
        client = ClientConnection(websocket)
        self.clients.append(client)
        
        client.task = asyncio.create_task(client.sender())

    def disconnect(self, websocket: WebSocket):
        for client in self.clients:
            if client.websocket == websocket:
                client.active = False

                if client.task:
                    client.task.cancel()

                self.clients.remove(client)
                break

    def subscribe(self, websocket:WebSocket, topic:str, group:str):
        print("[SUBSCRIBE] ", websocket, topic, group)
        for client in self.clients:
            if client.websocket == websocket:
                client.topics.add(topic)
                client.groups[topic] = group
                break
    
    def unsubscribe(self, websocket: WebSocket, topic: str):
        for client in self.clients:
            if client.websocket == websocket:
                client.topics.discard(topic)
                break
    
    async def broadcast(self, message: dict, topic: str):
        group_map = {}
        selected = None
        for client in self.clients:
            if topic not in client.topics:
                continue

            client_group = client.groups.get(topic, "default")
            group_map.setdefault(client_group, []).append(client)

        for group_name, clients in group_map.items():
            idx = self.group_index[group_name] % len(clients)
            selected = clients[idx]
            print(selected)
            self.group_index[group_name] += 1

            try:
                selected.queue.put_nowait(message)
            except asyncio.QueueFull:
                selected.active = False
                self.clients.remove(selected)
            except Exception as e:
                print(e)
