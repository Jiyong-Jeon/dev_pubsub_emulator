from __future__ import annotations

import logging

import grpc

from dev_pubsub.broker import MessageBroker
from dev_pubsub.generated.google.pubsub.v1 import pubsub_pb2_grpc
from dev_pubsub.publisher_servicer import PublisherServicer
from dev_pubsub.subscriber_servicer import SubscriberServicer

logger = logging.getLogger(__name__)


async def serve_grpc(broker: MessageBroker, port: int) -> grpc.aio.Server:
    server = grpc.aio.server()
    pubsub_pb2_grpc.add_PublisherServicer_to_server(
        PublisherServicer(broker), server
    )
    pubsub_pb2_grpc.add_SubscriberServicer_to_server(
        SubscriberServicer(broker), server
    )
    server.add_insecure_port(f"[::]:{port}")
    await server.start()
    logger.info("gRPC server listening on :%d", port)
    return server
