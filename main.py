"""Free-tier multiplayer starter. Configure secrets on the host, never in client HTML."""
import asyncio
import json
import math
import os
import re
import time
import urllib.request
from fastapi import FastAPI, WebSocket, WebSocketDisconnect

app = FastAPI()
ORIGIN = "https://jemoro.nl"
GAME_ID = os.environ.get("JEMORO_GAME_ID", "")
SERVER_KEY = os.environ.get("JEMORO_SERVER_KEY", "")
players = {}
last_broadcast = {}


async def jemoro(action, token, **extra):
    if not GAME_ID or not SERVER_KEY:
        raise ValueError("Configure JEMORO_GAME_ID and JEMORO_SERVER_KEY on the host")

    def request():
        body = json.dumps({"token": token, **extra}).encode()
        req = urllib.request.Request(
            f"{ORIGIN}/api/game-server/{GAME_ID}/{action}", data=body,
            headers={"Authorization": "Bearer " + SERVER_KEY, "Content-Type": "application/json"},
            method="POST",
        )
        with urllib.request.urlopen(req, timeout=12) as response:
            return json.load(response)

    return await asyncio.to_thread(request)


@app.get("/health")
def health():
    return {"ok": True, "configured": bool(GAME_ID and SERVER_KEY)}


async def broadcast(room):
    state = {"type": "state", "players": [
        {"id": p["id"], "x": p["x"], "y": p["y"], "health": p["health"], "skin": p["skin"]}
        for p in players.values() if p["room"] == room
    ]}
    for socket, p in list(players.items()):
        if p["room"] == room:
            try:
                await asyncio.wait_for(socket.send_json(state), timeout=1)
            except Exception:
                players.pop(socket, None)


@app.websocket("/ws")
async def websocket(socket: WebSocket):
    await socket.accept()
    if len(players) >= 128:
        await socket.close(code=1013)
        return
    room = "main"
    try:
        first = await asyncio.wait_for(socket.receive_text(), timeout=15)
        if len(first) > 2048:
            raise ValueError("Authentication too large")
        auth = json.loads(first)
        if auth.get("type") != "auth":
            raise ValueError("Authenticate first")
        token = auth.get("token", "")
        verified = await jemoro("session", token)
        room = auth.get("room", "main")
        if not isinstance(room, str) or not re.fullmatch(r"[a-z0-9-]{1,24}", room):
            raise ValueError("Invalid room")
        if sum(p["room"] == room for p in players.values()) >= 16:
            raise ValueError("Room full")
        # Reconnecting replaces the old socket for this authenticated player.
        for old, p in list(players.items()):
            if p["id"] == verified["playerId"]:
                players.pop(old, None)
                await old.close(code=1000)
        stamp = time.monotonic()
        player = {"id": verified["playerId"], "room": room, "x": 450.0, "y": 260.0,
                  "health": 50, "skin": "default", "inventory": verified["inventory"]}
        if len(players) >= 128:
            raise ValueError("Server full")
        players[socket] = player
        await socket.send_json({"type": "ready", "playerId": player["id"]})
        await broadcast(room)
        verified_at, moved_at, last_refresh = stamp, stamp, 0.0
        rate_at, rate_count = stamp, 0
        while True:
            raw = await asyncio.wait_for(socket.receive_text(), timeout=60)
            if len(raw) > 2048:
                raise ValueError("Message too large")
            now = time.monotonic()
            if now - rate_at >= 1:
                rate_at, rate_count = now, 0
            rate_count += 1
            if rate_count > 40:
                raise ValueError("Too many messages")
            data = json.loads(raw)
            if not isinstance(data, dict):
                raise ValueError("Invalid message")
            if data.get("type") == "auth":
                newer = await jemoro("session", data.get("token", ""))
                if newer["playerId"] != player["id"]:
                    raise ValueError("Account changed")
                token, verified_at = data["token"], now
                player["inventory"] = newer["inventory"]
            elif now - verified_at >= 60:
                newer = await jemoro("session", token)
                player["inventory"], verified_at = newer["inventory"], now
            if data.get("type") == "move":
                dx, dy = float(data.get("dx", 0)), float(data.get("dy", 0))
                if not math.isfinite(dx) or not math.isfinite(dy):
                    raise ValueError("Invalid movement")
                length = max(1, math.hypot(dx, dy))
                dt, moved_at = min(0.1, now - moved_at), now
                player["x"] = max(15, min(885, player["x"] + dx / length * 220 * dt))
                player["y"] = max(15, min(505, player["y"] + dy / length * 220 * dt))
            elif data.get("type") == "refresh_inventory" and now - last_refresh >= 2:
                result = await jemoro("session", token)
                player["inventory"], last_refresh = result["inventory"], now
            elif data.get("type") == "equip_blue":
                if any(i["productId"] == "blue-skin" and i["quantity"] > 0 for i in player["inventory"]):
                    player["skin"] = "blue"
            elif data.get("type") == "heal" and player["health"] < 100:
                # A client cannot set health, claim payment, or consume arbitrary stock.
                result = await jemoro("consume", token, productId="extra-life", requestId=data.get("requestId"))
                player["inventory"] = result["inventory"]
                if not result.get("alreadyApplied", False):
                    player["health"] = min(100, player["health"] + 25)
            if now - last_broadcast.get(room, 0) > 0.05:
                last_broadcast[room] = now
                await broadcast(room)
    except (WebSocketDisconnect, asyncio.TimeoutError):
        pass
    except Exception:
        try:
            await socket.send_json({"type": "error", "message": "Session ended. Reconnect or check server configuration."})
            await socket.close(code=1008)
        except Exception:
            pass
    finally:
        players.pop(socket, None)
        await broadcast(room)
        if not any(p["room"] == room for p in players.values()):
            last_broadcast.pop(room, None)
