"""
Ruta WebSocket del chat.

WS /ws/{token}

Protocolo (todos los mensajes son JSON):

Cliente → Servidor:
  { "type": "group_message", "content": "..." }
  { "type": "dm", "to": "<user_id>", "content": "..." }
  { "type": "ping" }

Servidor → Cliente:
  { "type": "group_message", "message": { ...ChatMessage } }
  { "type": "dm", "message": { ...ChatMessage } }
  { "type": "user_joined", "user": { ...ChatUser } }
  { "type": "user_left", "user_id": "..." }
  { "type": "users_list", "users": [ ...ChatUser ] }
  { "type": "group_history", "messages": [ ...ChatMessage ] }
  { "type": "pong" }
  { "type": "error", "message": "..." }
"""

import uuid
from datetime import datetime, timezone

from fastapi import APIRouter, WebSocket, WebSocketDisconnect
from fastapi.websockets import WebSocketState

from ..connection_manager import ConnectionManager
from ..models import ChatMessage

router = APIRouter()


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


async def _handle_message(
    manager: ConnectionManager,
    user_id: str,
    nickname: str,
    data: dict,
) -> None:
    
    msg_type = data.get("type")

    # ── Mensaje al chat grupal ────────────────────────────────────────────────
    if msg_type == "group_message":
        content = str(data.get("content", "")).strip()
        if not content:
            await manager.send_to(user_id, {"type": "error", "message": "El mensaje no puede estar vacío."})
            return

        if len(content) > 1000:
            await manager.send_to(user_id, {"type": "error", "message": "El mensaje es demasiado largo (máx 1000 caracteres)."})
            return

        msg = ChatMessage(
            id=str(uuid.uuid4()),
            sender_id=user_id,
            sender_nickname=nickname,
            content=content,
            type="group",
            recipient_id=None,
            timestamp=_now_iso(),
        )
        manager.save_group_message(msg)
        await manager.broadcast({
            "type": "group_message",
            "message": msg.model_dump(),
        })

    # ── Mensaje privado (DM) ──────────────────────────────────────────────────
    elif msg_type == "dm":
        to_id = str(data.get("to", "")).strip()
        content = str(data.get("content", "")).strip()

        if not to_id:
            await manager.send_to(user_id, {"type": "error", "message": "Debes especificar el destinatario ('to')."})
            return

        if to_id == user_id:
            await manager.send_to(user_id, {"type": "error", "message": "No puedes enviarte un DM a ti mismo."})
            return

        if not manager.get_user(to_id):
            await manager.send_to(user_id, {"type": "error", "message": "El destinatario no existe."})
            return

        if not content:
            await manager.send_to(user_id, {"type": "error", "message": "El mensaje no puede estar vacío."})
            return

        if len(content) > 1000:
            await manager.send_to(user_id, {"type": "error", "message": "El mensaje es demasiado largo (máx 1000 caracteres)."})
            return

        msg = ChatMessage(
            id=str(uuid.uuid4()),
            sender_id=user_id,
            sender_nickname=nickname,
            content=content,
            type="dm",
            recipient_id=to_id,
            timestamp=_now_iso(),
        )
        manager.save_dm_message(msg)

        # Enviamos al destinatario y una copia al remitente (para que vea su propio mensaje)
        payload = {"type": "dm", "message": msg.model_dump()}
        await manager.send_to(to_id, payload)
        await manager.send_to(user_id, payload)

    # ── Ping ──────────────────────────────────────────────────────────────────
    elif msg_type == "ping":
        await manager.send_to(user_id, {"type": "pong"})

    else:
        await manager.send_to(user_id, {"type": "error", "message": f"Tipo de mensaje desconocido: '{msg_type}'."})


@router.websocket("/ws/{token}")
async def websocket_endpoint(websocket: WebSocket, token: str) -> None:
 
    manager: ConnectionManager = websocket.app.state.manager

    # Validar token
    user_id = manager.decode_token(token)
    if not user_id or not manager.get_user(user_id):
        await websocket.close(code=4001, reason="Token inválido o usuario no registrado.")
        return

    # Conectar
    connected = await manager.connect(websocket, user_id)
    if not connected:
        await websocket.close(code=4001, reason="No se pudo conectar.")
        return

    user = manager.active_users[user_id]
    nickname = user.nickname

    # Notificar a todos los demás que este usuario entró
    await manager.broadcast(
        {"type": "user_joined", "user": user.model_dump()},
        exclude_id=user_id,
    )

    # Enviar estado inicial SOLO al nuevo usuario:
    # - Lista de usuarios online
    # - Historial del chat grupal
    await manager.send_to(user_id, {
        "type": "users_list",
        "users": [u.model_dump() for u in manager.get_online_users()],
    })
    await manager.send_to(user_id, {
        "type": "group_history",
        "messages": [m.model_dump() for m in manager.get_group_messages(50)],
    })

    # Loop principal de recepción de mensajes
    try:
        while True:
            data = await websocket.receive_json()
            await _handle_message(manager, user_id, nickname, data)

    except WebSocketDisconnect:
        pass

    except Exception:
        # Cualquier error inesperado — cerramos limpiamente
        if websocket.client_state == WebSocketState.CONNECTED:
            await websocket.close(code=1011, reason="Error interno del servidor.")

    finally:
        await manager.disconnect(user_id)
        await manager.broadcast({
            "type": "user_left",
            "user_id": user_id,
        })
