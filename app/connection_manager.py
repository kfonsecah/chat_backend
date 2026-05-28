

import base64
import uuid
from datetime import datetime, timezone
from typing import Optional

from fastapi import WebSocket

from .config import settings
from .models import ChatMessage, ChatUser


class ConnectionManager:
    def __init__(self) -> None:
        # WebSockets activos: user_id → WebSocket
        self.active_connections: dict[str, WebSocket] = {}

        # Usuarios conectados ahora: user_id → ChatUser
        self.active_users: dict[str, ChatUser] = {}

        # Todos los usuarios que alguna vez hicieron /join: user_id → ChatUser
        # (se limpian al reiniciar el servidor)
        self.registered_users: dict[str, ChatUser] = {}

        # Historial del chat grupal (últimos MAX_GROUP_MESSAGES mensajes)
        self.group_messages: list[ChatMessage] = []

        # Historial de DMs: frozenset({uid_a, uid_b}) → lista de mensajes
        self.dm_history: dict[frozenset, list[ChatMessage]] = {}

    # ── Tokens ───────────────────────────────────────────────────────────────

    def create_token(self, user_id: str) -> str:
        """Codifica user_id en base64 para usarlo como token de sesión."""
        return base64.urlsafe_b64encode(user_id.encode()).decode()

    def decode_token(self, token: str) -> Optional[str]:
        """Decodifica el token. Devuelve None si es inválido."""
        try:
            return base64.urlsafe_b64decode(token.encode()).decode()
        except Exception:
            return None

    # ── Registro ─────────────────────────────────────────────────────────────

    def register_user(self, nickname: str) -> tuple[ChatUser, str]:
    
        normalized = nickname.strip()

        # Retornar usuario existente si el nickname ya está registrado
        for existing in self.registered_users.values():
            if existing.nickname == normalized:
                return existing, self.create_token(existing.id)

        # Crear nuevo usuario
        user_id = str(uuid.uuid4())
        now = datetime.now(timezone.utc).isoformat()

        user = ChatUser(
            id=user_id,
            nickname=normalized,
            joined_at=now,
            is_online=False,
        )

        self.registered_users[user_id] = user
        token = self.create_token(user_id)
        return user, token

    # ── Ciclo de vida WebSocket ───────────────────────────────────────────────

    async def connect(self, websocket: WebSocket, user_id: str) -> bool:
        
        user = self.registered_users.get(user_id)
        if not user:
            return False

        await websocket.accept()
        user.is_online = True
        self.active_connections[user_id] = websocket
        self.active_users[user_id] = user
        return True

    async def disconnect(self, user_id: str) -> None:
       
        self.active_connections.pop(user_id, None)
        user = self.active_users.pop(user_id, None)
        if user:
            user.is_online = False

    # ── Envío de mensajes ─────────────────────────────────────────────────────

    async def broadcast(self, message: dict, exclude_id: Optional[str] = None) -> None:
       
        for uid, ws in list(self.active_connections.items()):
            if uid != exclude_id:
                try:
                    await ws.send_json(message)
                except Exception:
                    # Si falla el envío, ignoramos — el disconnect se detectará
                    # en el próximo receive del loop principal
                    pass

    async def send_to(self, user_id: str, message: dict) -> None:
       
        ws = self.active_connections.get(user_id)
        if ws:
            try:
                await ws.send_json(message)
            except Exception:
                pass

    # ── Historial ─────────────────────────────────────────────────────────────

    def save_group_message(self, msg: ChatMessage) -> None:
        self.group_messages.append(msg)
        # Mantener solo los últimos N mensajes
        if len(self.group_messages) > settings.max_group_messages:
            self.group_messages = self.group_messages[-settings.max_group_messages:]

    def save_dm_message(self, msg: ChatMessage) -> None:
        if not msg.recipient_id:
            return
        key = frozenset({msg.sender_id, msg.recipient_id})
        history = self.dm_history.setdefault(key, [])
        history.append(msg)
        if len(history) > settings.max_dm_messages:
            self.dm_history[key] = history[-settings.max_dm_messages:]

    def get_group_messages(self, limit: int = 50) -> list[ChatMessage]:
        return self.group_messages[-limit:]

    def get_dm_history(
        self, user_a: str, user_b: str, limit: int = 50
    ) -> list[ChatMessage]:
        key = frozenset({user_a, user_b})
        return self.dm_history.get(key, [])[-limit:]

    # ── Consultas ─────────────────────────────────────────────────────────────

    def get_online_users(self) -> list[ChatUser]:
        return list(self.active_users.values())

    def get_user(self, user_id: str) -> Optional[ChatUser]:
        return self.registered_users.get(user_id)
