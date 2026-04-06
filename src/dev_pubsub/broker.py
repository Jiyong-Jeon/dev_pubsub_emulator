from __future__ import annotations

import asyncio
import logging
import time
import uuid
from dataclasses import dataclass, field
from typing import Any

from typing import Callable

from dev_pubsub.config import Config
from dev_pubsub.filter import parse_filter

logger = logging.getLogger(__name__)


@dataclass
class StoredMessage:
    message_id: str
    data: bytes
    attributes: dict[str, str]
    publish_time_seconds: float
    ordering_key: str = ""


@dataclass
class DeliveredMessage:
    ack_id: str
    message: StoredMessage
    delivery_attempt: int
    ack_deadline: float  # absolute timestamp


@dataclass
class SubscriptionState:
    name: str  # full resource path
    topic: str  # full resource path
    ack_deadline_seconds: int = 10
    filter_expr: str | None = None
    filter_fn: Callable[[dict[str, str]], bool] = field(
        default_factory=lambda: lambda attrs: True
    )
    pending: list[StoredMessage] = field(default_factory=list)
    outstanding: dict[str, DeliveredMessage] = field(default_factory=dict)  # ack_id -> msg
    streams: list[asyncio.Queue] = field(default_factory=list)
    _stream_index: int = 0  # round-robin index


@dataclass
class TopicState:
    name: str  # full resource path
    subscriptions: list[str] = field(default_factory=list)  # subscription paths
    labels: dict[str, str] = field(default_factory=dict)


class MessageBroker:
    def __init__(self, config: Config) -> None:
        self.config = config
        self.topics: dict[str, TopicState] = {}
        self.subscriptions: dict[str, SubscriptionState] = {}
        self._lock = asyncio.Lock()

        # Stats
        self.stats = {"published": 0, "delivered": 0, "acked": 0}

        # Message history for dashboard
        self.message_history: list[dict[str, Any]] = []
        self._max_history = 500

        # WebSocket broadcast callback
        self._ws_broadcast: Any = None

        self._init_from_config()

    def _init_from_config(self) -> None:
        for tc in self.config.topics:
            topic_path = self.config.topic_path(tc.name)
            self._ensure_topic(topic_path)
            for sub_cfg in tc.subscriptions:
                sub_path = self.config.subscription_path(sub_cfg.name)
                self._ensure_subscription(sub_path, topic_path, filter_expr=sub_cfg.filter)

    def _ensure_topic(self, topic_path: str) -> TopicState:
        if topic_path not in self.topics:
            self.topics[topic_path] = TopicState(name=topic_path)
            logger.info("Created topic: %s", topic_path)
        return self.topics[topic_path]

    def _ensure_subscription(
        self,
        sub_path: str,
        topic_path: str,
        ack_deadline_seconds: int = 10,
        filter_expr: str | None = None,
    ) -> SubscriptionState:
        if sub_path not in self.subscriptions:
            topic = self._ensure_topic(topic_path)
            sub = SubscriptionState(
                name=sub_path,
                topic=topic_path,
                ack_deadline_seconds=ack_deadline_seconds,
                filter_expr=filter_expr,
                filter_fn=parse_filter(filter_expr),
            )
            self.subscriptions[sub_path] = sub
            if sub_path not in topic.subscriptions:
                topic.subscriptions.append(sub_path)
            logger.info("Created subscription: %s -> %s", sub_path, topic_path)
        return self.subscriptions[sub_path]

    async def publish(self, topic_path: str, messages: list[dict]) -> list[str]:
        async with self._lock:
            topic = self._ensure_topic(topic_path)
            message_ids = []

            for msg_data in messages:
                msg_id = str(uuid.uuid4())[:8]
                stored = StoredMessage(
                    message_id=msg_id,
                    data=msg_data.get("data", b""),
                    attributes=dict(msg_data.get("attributes", {})),
                    publish_time_seconds=time.time(),
                    ordering_key=msg_data.get("ordering_key", ""),
                )
                message_ids.append(msg_id)
                self.stats["published"] += 1

                # Fan-out to matching subscriptions (apply filter)
                for sub_path in topic.subscriptions:
                    sub = self.subscriptions.get(sub_path)
                    if sub is None:
                        continue
                    if not sub.filter_fn(stored.attributes):
                        continue
                    if sub.streams:
                        # Deliver to ONE stream (round-robin)
                        ack_id = str(uuid.uuid4())
                        delivered = DeliveredMessage(
                            ack_id=ack_id,
                            message=stored,
                            delivery_attempt=1,
                            ack_deadline=time.time() + sub.ack_deadline_seconds,
                        )
                        sub.outstanding[ack_id] = delivered
                        self.stats["delivered"] += 1
                        self._send_to_one_stream(sub, delivered)
                    else:
                        sub.pending.append(stored)

                self._record_message(topic_path, stored)

            return message_ids

    def pull(self, sub_path: str, max_messages: int = 100) -> list[DeliveredMessage]:
        sub = self.subscriptions.get(sub_path)
        if sub is None:
            return []

        result = []
        count = min(max_messages, len(sub.pending))
        for _ in range(count):
            stored = sub.pending.pop(0)
            ack_id = str(uuid.uuid4())
            delivered = DeliveredMessage(
                ack_id=ack_id,
                message=stored,
                delivery_attempt=1,
                ack_deadline=time.time() + sub.ack_deadline_seconds,
            )
            sub.outstanding[ack_id] = delivered
            self.stats["delivered"] += 1
            result.append(delivered)

        return result

    def acknowledge(self, sub_path: str, ack_ids: list[str]) -> None:
        sub = self.subscriptions.get(sub_path)
        if sub is None:
            return
        for ack_id in ack_ids:
            delivered = sub.outstanding.pop(ack_id, None)
            if delivered is None:
                continue
            self.stats["acked"] += 1
            # Update history record
            msg_id = delivered.message.message_id
            for record in reversed(self.message_history):
                if record["message_id"] == msg_id:
                    record["acked"] = True
                    break
            if self._ws_broadcast:
                asyncio.ensure_future(self._ws_broadcast({
                    "type": "ack",
                    "message_id": msg_id,
                    "subscription": sub_path,
                }))

    def modify_ack_deadline(
        self, sub_path: str, ack_ids: list[str], deadline_seconds: int
    ) -> None:
        sub = self.subscriptions.get(sub_path)
        if sub is None:
            return
        for ack_id in ack_ids:
            delivered = sub.outstanding.get(ack_id)
            if delivered is None:
                continue
            if deadline_seconds == 0:
                # nack: re-deliver
                del sub.outstanding[ack_id]
                self._redeliver(sub, delivered.message)
            else:
                delivered.ack_deadline = time.time() + deadline_seconds

    def register_stream(self, sub_path: str) -> asyncio.Queue | None:
        sub = self.subscriptions.get(sub_path)
        if sub is None:
            return None
        q: asyncio.Queue = asyncio.Queue(maxsize=1000)
        sub.streams.append(q)

        # Flush pending messages to the new stream
        while sub.pending:
            stored = sub.pending.pop(0)
            ack_id = str(uuid.uuid4())
            delivered = DeliveredMessage(
                ack_id=ack_id,
                message=stored,
                delivery_attempt=1,
                ack_deadline=time.time() + sub.ack_deadline_seconds,
            )
            sub.outstanding[ack_id] = delivered
            self.stats["delivered"] += 1
            try:
                q.put_nowait(delivered)
            except asyncio.QueueFull:
                break

        return q

    def unregister_stream(self, sub_path: str, q: asyncio.Queue) -> None:
        sub = self.subscriptions.get(sub_path)
        if sub is None:
            return
        if q in sub.streams:
            sub.streams.remove(q)

    def _send_to_one_stream(self, sub: SubscriptionState, delivered: DeliveredMessage) -> None:
        """Send a message to exactly one stream via round-robin."""
        for _ in range(len(sub.streams)):
            idx = sub._stream_index % len(sub.streams)
            sub._stream_index += 1
            try:
                sub.streams[idx].put_nowait(delivered)
                return
            except asyncio.QueueFull:
                continue
        # All streams full — message stays in outstanding, will be re-delivered on deadline

    def _redeliver(self, sub: SubscriptionState, msg: StoredMessage) -> None:
        if sub.streams:
            ack_id = str(uuid.uuid4())
            delivered = DeliveredMessage(
                ack_id=ack_id,
                message=msg,
                delivery_attempt=2,
                ack_deadline=time.time() + sub.ack_deadline_seconds,
            )
            sub.outstanding[ack_id] = delivered
            self.stats["delivered"] += 1
            self._send_to_one_stream(sub, delivered)
        else:
            sub.pending.append(msg)

    async def run_deadline_checker(self) -> None:
        while True:
            await asyncio.sleep(1)
            now = time.time()
            for sub in list(self.subscriptions.values()):
                expired = [
                    (aid, dm)
                    for aid, dm in list(sub.outstanding.items())
                    if dm.ack_deadline < now
                ]
                for ack_id, delivered in expired:
                    del sub.outstanding[ack_id]
                    self._redeliver(sub, delivered.message)
                    logger.debug(
                        "Re-delivered expired message %s in %s",
                        delivered.message.message_id,
                        sub.name,
                    )

    def _record_message(self, topic_path: str, msg: StoredMessage) -> None:
        record = {
            "type": "message",
            "topic": topic_path,
            "message_id": msg.message_id,
            "data": msg.data.decode("utf-8", errors="replace") if msg.data else "",
            "data_size": len(msg.data),
            "attributes": msg.attributes,
            "timestamp": msg.publish_time_seconds,
            "acked": False,
        }
        self.message_history.append(record)
        if len(self.message_history) > self._max_history:
            self.message_history = self.message_history[-self._max_history:]

        if self._ws_broadcast:
            asyncio.ensure_future(self._ws_broadcast(record))

    def get_stats(self) -> dict:
        return {
            **self.stats,
            "topics": len(self.topics),
            "subscriptions": len(self.subscriptions),
            "pending": sum(len(s.pending) for s in self.subscriptions.values()),
            "outstanding": sum(len(s.outstanding) for s in self.subscriptions.values()),
        }
