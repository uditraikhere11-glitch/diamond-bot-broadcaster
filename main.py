"""
Diamond-Bot Pure Signal Broadcaster (In-House V2)
Listens to Telegram -> Broadcasts via WebSockets to local bots.
"""
import os, json, uuid, asyncio, time
import logging
from contextlib import asynccontextmanager
from fastapi import FastAPI, WebSocket, WebSocketDisconnect, Query
from starlette.websockets import WebSocketState
from telethon import TelegramClient, events
from dotenv import load_dotenv

load_dotenv()
logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger("broadcaster")

# Credentials
API_ID = int(os.environ.get("TG_API_ID", 0))
API_HASH = os.environ.get("TG_API_HASH", "")
SESSION_STRING = os.environ.get("TG_SESSION_STRING", "")
PORT = int(os.environ.get("PORT", 8001))

class ConnectionManager:
    def __init__(self):
        self.active_connections: list[WebSocket] = []

    async def connect(self, websocket: WebSocket):
        await websocket.accept()
        self.active_connections.append(websocket)
        logger.info(f"Bot connected. Total active: {len(self.active_connections)}")

    def disconnect(self, websocket: WebSocket):
        if websocket in self.active_connections:
            self.active_connections.remove(websocket)
            logger.warning(f"Bot disconnected. Total active: {len(self.active_connections)}")

    async def broadcast(self, data: dict) -> int:
        delivered = 0
        broken = []
        for ws in self.active_connections:
            try:
                if ws.client_state == WebSocketState.CONNECTED:
                    await ws.send_json(data)
                    delivered += 1
            except Exception:
                broken.append(ws)
        for ws in broken:
            self.disconnect(ws)
        return delivered

manager = ConnectionManager()
tg_client = TelegramClient("broadcaster_session", API_ID, API_HASH) if not SESSION_STRING else \
            TelegramClient(__import__('telethon').sessions.StringSession(SESSION_STRING), API_ID, API_HASH)

async def start_telegram():
    if not API_ID: return
    logger.info("Starting Telegram listener...")
    
    @tg_client.on(events.NewMessage)
    async def handler(event):
        # We broadcast the raw text; the execution bots handle regex/parsing locally
        if not event.message.text: return
        
        payload = {
            "signal_id": f"LIVE_{event.chat_id}_{event.message.id}",
            "source": "TELEGRAM",
            "chat_id": event.chat_id,
            "raw_text": event.message.text,
            "timestamp": time.time()
        }
        await manager.broadcast(payload)
        logger.info(f"Broadcasted signal from {event.chat_id}")

    await tg_client.start()
    await tg_client.get_dialogs() # Pre-warm dialogs to fix the "Test" group issue
    logger.info("Dialogs cached. Listening for signals...")

@asynccontextmanager
async def lifespan(app: FastAPI):
    tg_task = asyncio.create_task(start_telegram())
    yield
    tg_task.cancel()
    await tg_client.disconnect()

app = FastAPI(title="Diamond-Bot Broadcaster", lifespan=lifespan)

@app.post("/test-signal")
async def test_signal(symbol: str = Query("YESBANK"), asset_class: str = Query("EQUITY")):
    payload = {
        "signal_id": f"TEST_{int(time.time())}",
        "source": "SYNTHETIC_TEST",
        "asset_class": asset_class.upper(),
        "symbol": symbol.upper(),
        "raw_text": f"BUY {symbol.upper()} MARKET"
    }
    delivered = await manager.broadcast(payload)
    return {"status": "success", "delivered_to": delivered}

@app.websocket("/ws")
async def websocket_endpoint(websocket: WebSocket):
    await manager.connect(websocket)
    try:
        while True:
            data = await websocket.receive_text()
            if data.startswith("{"):
                msg = json.loads(data)
                if msg.get("type") == "ACK":
                    logger.info(f"[ACK VERIFIED] Signal {msg.get('signal_id')} received by {msg.get('client', 'Bot')}")
    except WebSocketDisconnect:
        pass
    finally:
        manager.disconnect(websocket)

if __name__ == "__main__":
    import uvicorn
    uvicorn.run("main:app", host="127.0.0.1", port=PORT)
