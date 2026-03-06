from __future__ import annotations

import logging

import grpc
from google.protobuf import empty_pb2

from dev_pubsub.broker import MessageBroker
from dev_pubsub.generated.google.pubsub.v1 import pubsub_pb2, pubsub_pb2_grpc

logger = logging.getLogger(__name__)


class PublisherServicer(pubsub_pb2_grpc.PublisherServicer):
    def __init__(self, broker: MessageBroker) -> None:
        self.broker = broker

    async def CreateTopic(self, request, context):
        topic = self.broker._ensure_topic(request.name)
        if request.labels:
            topic.labels.update(request.labels)
        return pubsub_pb2.Topic(name=topic.name, labels=topic.labels)

    async def GetTopic(self, request, context):
        topic = self.broker._ensure_topic(request.topic)
        return pubsub_pb2.Topic(name=topic.name, labels=topic.labels)

    async def Publish(self, request, context):
        topic_path = request.topic
        self.broker._ensure_topic(topic_path)

        messages = []
        for msg in request.messages:
            messages.append({
                "data": msg.data,
                "attributes": dict(msg.attributes),
                "ordering_key": msg.ordering_key,
            })

        message_ids = await self.broker.publish(topic_path, messages)
        logger.info(
            "Published %d message(s) to %s", len(message_ids), topic_path
        )
        return pubsub_pb2.PublishResponse(message_ids=message_ids)

    async def ListTopics(self, request, context):
        topics = [
            pubsub_pb2.Topic(name=t.name, labels=t.labels)
            for t in self.broker.topics.values()
            if t.name.startswith(f"projects/{request.project}/")
            or request.project in t.name
        ]
        return pubsub_pb2.ListTopicsResponse(topics=topics)

    async def ListTopicSubscriptions(self, request, context):
        topic = self.broker.topics.get(request.topic)
        subs = topic.subscriptions if topic else []
        return pubsub_pb2.ListTopicSubscriptionsResponse(subscriptions=subs)

    async def DeleteTopic(self, request, context):
        self.broker.topics.pop(request.topic, None)
        logger.info("Deleted topic: %s", request.topic)
        return empty_pb2.Empty()

    async def UpdateTopic(self, request, context):
        topic = self.broker._ensure_topic(request.topic.name)
        if request.topic.labels:
            topic.labels.update(request.topic.labels)
        return pubsub_pb2.Topic(name=topic.name, labels=topic.labels)

    async def DetachSubscription(self, request, context):
        return pubsub_pb2.DetachSubscriptionResponse()

    async def ListTopicSnapshots(self, request, context):
        return pubsub_pb2.ListTopicSnapshotsResponse()
