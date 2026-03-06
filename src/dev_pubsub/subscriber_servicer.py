from __future__ import annotations

import asyncio
import logging
import time

import grpc
from google.protobuf import duration_pb2, empty_pb2, timestamp_pb2

from dev_pubsub.broker import DeliveredMessage, MessageBroker
from dev_pubsub.generated.google.pubsub.v1 import pubsub_pb2, pubsub_pb2_grpc

logger = logging.getLogger(__name__)


def _to_received_message(dm: DeliveredMessage) -> pubsub_pb2.ReceivedMessage:
    ts = timestamp_pb2.Timestamp()
    ts.FromSeconds(int(dm.message.publish_time_seconds))
    return pubsub_pb2.ReceivedMessage(
        ack_id=dm.ack_id,
        message=pubsub_pb2.PubsubMessage(
            data=dm.message.data,
            attributes=dm.message.attributes,
            message_id=dm.message.message_id,
            publish_time=ts,
            ordering_key=dm.message.ordering_key,
        ),
        delivery_attempt=dm.delivery_attempt,
    )


class SubscriberServicer(pubsub_pb2_grpc.SubscriberServicer):
    def __init__(self, broker: MessageBroker) -> None:
        self.broker = broker

    async def CreateSubscription(self, request, context):
        sub = self.broker._ensure_subscription(
            request.name,
            request.topic,
            ack_deadline_seconds=request.ack_deadline_seconds or 10,
        )
        return pubsub_pb2.Subscription(
            name=sub.name,
            topic=sub.topic,
            ack_deadline_seconds=sub.ack_deadline_seconds,
        )

    async def GetSubscription(self, request, context):
        sub = self.broker.subscriptions.get(request.subscription)
        if sub is None:
            context.set_code(grpc.StatusCode.NOT_FOUND)
            context.set_details(f"Subscription not found: {request.subscription}")
            return pubsub_pb2.Subscription()
        return pubsub_pb2.Subscription(
            name=sub.name,
            topic=sub.topic,
            ack_deadline_seconds=sub.ack_deadline_seconds,
        )

    async def Pull(self, request, context):
        delivered = self.broker.pull(
            request.subscription, max_messages=request.max_messages or 100
        )
        received = [_to_received_message(dm) for dm in delivered]
        return pubsub_pb2.PullResponse(received_messages=received)

    async def Acknowledge(self, request, context):
        self.broker.acknowledge(request.subscription, list(request.ack_ids))
        return empty_pb2.Empty()

    async def ModifyAckDeadline(self, request, context):
        self.broker.modify_ack_deadline(
            request.subscription,
            list(request.ack_ids),
            request.ack_deadline_seconds,
        )
        return empty_pb2.Empty()

    async def StreamingPull(self, request_iterator, context):
        sub_path = None
        stream_queue = None
        ack_deadline = 10

        try:
            # Read initial request
            first_request = await context.read()
            if first_request is None:
                return

            sub_path = first_request.subscription
            ack_deadline = first_request.stream_ack_deadline_seconds or 10

            sub = self.broker.subscriptions.get(sub_path)
            if sub is None:
                # Auto-create if possible (emulator behavior)
                context.set_code(grpc.StatusCode.NOT_FOUND)
                context.set_details(f"Subscription not found: {sub_path}")
                return

            stream_queue = self.broker.register_stream(sub_path)
            if stream_queue is None:
                return

            logger.info("StreamingPull connected: %s", sub_path)

            async def recv_loop():
                while True:
                    req = await context.read()
                    if req is None:
                        break
                    if req.ack_ids:
                        self.broker.acknowledge(sub_path, list(req.ack_ids))
                    if req.modify_deadline_ack_ids:
                        self.broker.modify_ack_deadline(
                            sub_path,
                            list(req.modify_deadline_ack_ids),
                            req.modify_deadline_seconds[0]
                            if req.modify_deadline_seconds
                            else ack_deadline,
                        )

            async def send_loop():
                while True:
                    try:
                        delivered = await asyncio.wait_for(
                            stream_queue.get(), timeout=30
                        )
                        resp = pubsub_pb2.StreamingPullResponse(
                            received_messages=[_to_received_message(delivered)],
                        )
                        await context.write(resp)
                    except asyncio.TimeoutError:
                        # Send empty keepalive-like response
                        continue
                    except Exception:
                        break

            recv_task = asyncio.create_task(recv_loop())
            send_task = asyncio.create_task(send_loop())

            done, pending = await asyncio.wait(
                [recv_task, send_task], return_when=asyncio.FIRST_COMPLETED
            )
            for task in pending:
                task.cancel()
                try:
                    await task
                except (asyncio.CancelledError, Exception):
                    pass

        except Exception as e:
            logger.debug("StreamingPull ended: %s (%s)", sub_path, e)
        finally:
            if stream_queue and sub_path:
                self.broker.unregister_stream(sub_path, stream_queue)
                logger.info("StreamingPull disconnected: %s", sub_path)

    async def DeleteSubscription(self, request, context):
        sub = self.broker.subscriptions.pop(request.subscription, None)
        if sub:
            topic = self.broker.topics.get(sub.topic)
            if topic and sub.name in topic.subscriptions:
                topic.subscriptions.remove(sub.name)
            logger.info("Deleted subscription: %s", request.subscription)
        return empty_pb2.Empty()

    async def ListSubscriptions(self, request, context):
        subs = [
            pubsub_pb2.Subscription(
                name=s.name,
                topic=s.topic,
                ack_deadline_seconds=s.ack_deadline_seconds,
            )
            for s in self.broker.subscriptions.values()
            if request.project in s.name
        ]
        return pubsub_pb2.ListSubscriptionsResponse(subscriptions=subs)

    async def UpdateSubscription(self, request, context):
        sub = self.broker.subscriptions.get(request.subscription.name)
        if sub and request.subscription.ack_deadline_seconds:
            sub.ack_deadline_seconds = request.subscription.ack_deadline_seconds
        if sub is None:
            context.set_code(grpc.StatusCode.NOT_FOUND)
            return pubsub_pb2.Subscription()
        return pubsub_pb2.Subscription(
            name=sub.name,
            topic=sub.topic,
            ack_deadline_seconds=sub.ack_deadline_seconds,
        )

    async def ModifyPushConfig(self, request, context):
        return empty_pb2.Empty()

    async def GetSnapshot(self, request, context):
        context.set_code(grpc.StatusCode.UNIMPLEMENTED)
        return pubsub_pb2.Snapshot()

    async def ListSnapshots(self, request, context):
        return pubsub_pb2.ListSnapshotsResponse()

    async def CreateSnapshot(self, request, context):
        context.set_code(grpc.StatusCode.UNIMPLEMENTED)
        return pubsub_pb2.Snapshot()

    async def UpdateSnapshot(self, request, context):
        context.set_code(grpc.StatusCode.UNIMPLEMENTED)
        return pubsub_pb2.Snapshot()

    async def DeleteSnapshot(self, request, context):
        return empty_pb2.Empty()

    async def Seek(self, request, context):
        return pubsub_pb2.SeekResponse()
