from __future__ import annotations

import asyncio
import json
import logging
from pathlib import Path

from fastapi import FastAPI, WebSocket, WebSocketDisconnect
from fastapi.responses import HTMLResponse, JSONResponse

from dev_pubsub.broker import MessageBroker

logger = logging.getLogger(__name__)

STATIC_DIR = Path(__file__).parent / "static"


def create_app(broker: MessageBroker) -> FastAPI:
    app = FastAPI(title="Dev PubSub Dashboard")

    ws_clients: list[WebSocket] = []

    async def broadcast(data: dict) -> None:
        message = json.dumps(data, default=str)
        disconnected = []
        for ws in ws_clients:
            try:
                await ws.send_text(message)
            except Exception:
                disconnected.append(ws)
        for ws in disconnected:
            ws_clients.remove(ws)

    broker._ws_broadcast = broadcast

    @app.get("/", response_class=HTMLResponse)
    async def dashboard():
        html_file = STATIC_DIR / "index.html"
        return HTMLResponse(content=html_file.read_text())

    @app.get("/api/topics")
    async def list_topics():
        topics = []
        for t in broker.topics.values():
            topics.append({
                "name": t.name,
                "subscriptions": t.subscriptions,
                "labels": t.labels,
            })
        return JSONResponse(topics)

    @app.get("/api/subscriptions")
    async def list_subscriptions():
        subs = []
        for s in broker.subscriptions.values():
            subs.append({
                "name": s.name,
                "topic": s.topic,
                "pending": len(s.pending),
                "outstanding": len(s.outstanding),
                "streams": len(s.streams),
                "ack_deadline_seconds": s.ack_deadline_seconds,
            })
        return JSONResponse(subs)

    @app.get("/api/stats")
    async def get_stats():
        return JSONResponse(broker.get_stats())

    @app.get("/api/messages")
    async def get_messages():
        return JSONResponse(broker.message_history[-100:])

    @app.post("/api/publish/{topic_name}")
    async def publish_message(topic_name: str, body: dict):
        topic_path = None
        for t in broker.topics:
            if t.endswith(f"/{topic_name}"):
                topic_path = t
                break
        if topic_path is None:
            topic_path = f"projects/{broker.config.project_id}/topics/{topic_name}"
            broker._ensure_topic(topic_path)

        data = body.get("data", "").encode("utf-8")
        attributes = body.get("attributes", {})

        ids = await broker.publish(topic_path, [{"data": data, "attributes": attributes}])
        return JSONResponse({"message_ids": ids})

    @app.websocket("/ws")
    async def websocket_endpoint(ws: WebSocket):
        await ws.accept()
        ws_clients.append(ws)
        logger.info("WebSocket client connected (%d total)", len(ws_clients))
        try:
            while True:
                await ws.receive_text()
        except WebSocketDisconnect:
            pass
        finally:
            if ws in ws_clients:
                ws_clients.remove(ws)
            logger.info("WebSocket client disconnected (%d total)", len(ws_clients))

    return app
