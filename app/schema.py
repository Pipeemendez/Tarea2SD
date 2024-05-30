from typing import Optional
from pydantic import BaseModel, EmailStr

class Message(BaseModel):
    id: Optional[int] = None
    nombre: str
    precio: float
    correo: EmailStr
    estado: Optional[str] = None