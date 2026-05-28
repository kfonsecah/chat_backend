from typing import Literal, Optional

from pydantic import BaseModel


class ChatUser(BaseModel):
    
    id: str
    nickname: str
    joined_at: str       # ISO 8601
    is_online: bool


class ChatMessage(BaseModel):
    """Mensaje del chat — grupal o DM."""
    id: str
    sender_id: str
    sender_nickname: str
    content: str
    type: Literal["group", "dm"]
    recipient_id: Optional[str] = None   # Solo presente en DMs
    timestamp: str                        # ISO 8601


# ── Payloads HTTP ────────────────────────────────────────────────────────────

class JoinRequest(BaseModel):
    nickname: str


class JoinResponse(BaseModel):
    user: ChatUser
    token: str


# ── Payloads WebSocket (cliente → servidor) ───────────────────────────────────

class WsGroupMessage(BaseModel):
    type: Literal["group_message"]
    content: str


class WsDMMessage(BaseModel):
    type: Literal["dm"]
    to: str       # user_id del destinatario
    content: str


class WsPing(BaseModel):
    type: Literal["ping"]
