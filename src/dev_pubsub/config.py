from __future__ import annotations

import argparse
from dataclasses import dataclass, field
from pathlib import Path

import yaml


@dataclass
class SubscriptionConfig:
    name: str
    filter: str | None = None


@dataclass
class TopicConfig:
    name: str
    subscriptions: list[SubscriptionConfig] = field(default_factory=list)


@dataclass
class Config:
    project_id: str = "postmath-dev"
    grpc_port: int = 8085
    web_port: int = 8086
    topics: list[TopicConfig] = field(default_factory=list)

    def topic_path(self, name: str) -> str:
        return f"projects/{self.project_id}/topics/{name}"

    def subscription_path(self, name: str) -> str:
        return f"projects/{self.project_id}/subscriptions/{name}"


def load_config(path: str | Path | None = None) -> Config:
    if path is None:
        path = Path("config.yaml")
    else:
        path = Path(path)

    if not path.exists():
        return Config()

    with open(path) as f:
        raw = yaml.safe_load(f) or {}

    topics = []
    for t in raw.get("topics", []):
        subs = []
        for s in t.get("subscriptions", []):
            if isinstance(s, str):
                subs.append(SubscriptionConfig(name=s))
            elif isinstance(s, dict):
                subs.append(SubscriptionConfig(
                    name=s["name"],
                    filter=s.get("filter"),
                ))
        topics.append(TopicConfig(
            name=t["name"],
            subscriptions=subs,
        ))

    return Config(
        project_id=raw.get("project_id", "postmath-dev"),
        grpc_port=raw.get("grpc_port", 8085),
        web_port=raw.get("web_port", 8086),
        topics=topics,
    )


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Dev PubSub Emulator")
    parser.add_argument("--config", default="config.yaml", help="Path to config YAML")
    return parser.parse_args()
