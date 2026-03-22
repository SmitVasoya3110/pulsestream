from pydantic import BaseModel

class Event(BaseModel):
    topic: str
    data: dict
    

