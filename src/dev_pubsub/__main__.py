from __future__ import annotations

import asyncio
import logging
import signal

import uvicorn

from dev_pubsub.broker import MessageBroker
from dev_pubsub.config import load_config, parse_args
from dev_pubsub.grpc_server import serve_grpc
from dev_pubsub.web_server import create_app

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    datefmt="%H:%M:%S",
)
logger = logging.getLogger("dev_pubsub")


async def run() -> None:
    args = parse_args()
    config = load_config(args.config)

    broker = MessageBroker(config)

    logger.info("Project: %s", config.project_id)
    logger.info("Topics: %s", [t.name for t in config.topics])

    # Start gRPC server
    grpc_server = await serve_grpc(broker, config.grpc_port)

    # Start FastAPI server
    app = create_app(broker)
    uvi_config = uvicorn.Config(
        app,
        host="0.0.0.0",
        port=config.web_port,
        log_level="warning",
    )
    uvi_server = uvicorn.Server(uvi_config)

    logger.info("Dashboard: http://localhost:%d", config.web_port)

    # Setup shutdown
    stop_event = asyncio.Event()

    def _signal_handler():
        logger.info("Shutting down...")
        stop_event.set()

    loop = asyncio.get_running_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        loop.add_signal_handler(sig, _signal_handler)

    # Run all tasks
    deadline_task = asyncio.create_task(broker.run_deadline_checker())
    uvi_task = asyncio.create_task(uvi_server.serve())
    stop_task = asyncio.create_task(stop_event.wait())

    done, pending = await asyncio.wait(
        [deadline_task, uvi_task, stop_task],
        return_when=asyncio.FIRST_COMPLETED,
    )

    # Cleanup
    for task in pending:
        task.cancel()
        try:
            await task
        except (asyncio.CancelledError, Exception):
            pass

    await grpc_server.stop(grace=2)
    logger.info("Server stopped.")


def main() -> None:
    asyncio.run(run())


if __name__ == "__main__":
    main()
