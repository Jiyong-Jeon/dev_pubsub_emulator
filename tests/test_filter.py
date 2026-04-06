from __future__ import annotations

import asyncio

from dev_pubsub.filter import parse_filter
from dev_pubsub.config import Config, SubscriptionConfig, TopicConfig, load_config
from dev_pubsub.broker import MessageBroker


# ── parse_filter unit tests ──────────────────────────────────────────


class TestParseFilter:
    def test_none_returns_always_true(self):
        fn = parse_filter(None)
        assert fn({}) is True
        assert fn({"service_name": "dps"}) is True

    def test_empty_string_returns_always_true(self):
        fn = parse_filter("")
        assert fn({}) is True

    def test_equality_match(self):
        fn = parse_filter('attributes.service_name = "dps"')
        assert fn({"service_name": "dps"}) is True
        assert fn({"service_name": "cms"}) is False
        assert fn({"service_name": "ags"}) is False

    def test_equality_no_match(self):
        fn = parse_filter('attributes.type = "requested"')
        assert fn({"type": "processing"}) is False
        assert fn({"type": "requested"}) is True

    def test_missing_attribute_returns_false(self):
        fn = parse_filter('attributes.service_name = "dps"')
        assert fn({}) is False
        assert fn({"other_key": "dps"}) is False

    def test_unsupported_expression_returns_always_true(self):
        fn = parse_filter("NOT attributes:service_name")
        assert fn({}) is True
        assert fn({"service_name": "dps"}) is True

    def test_whitespace_tolerance(self):
        fn = parse_filter('  attributes.service_name  =  "cms"  ')
        assert fn({"service_name": "cms"}) is True
        assert fn({"service_name": "dps"}) is False


# ── Config parsing tests ─────────────────────────────────────────────


class TestConfigParsing:
    def test_load_config_plain_strings(self, tmp_path):
        cfg_file = tmp_path / "config.yaml"
        cfg_file.write_text(
            "project_id: test\n"
            "topics:\n"
            "  - name: my-topic\n"
            "    subscriptions:\n"
            "      - my-sub\n"
        )
        cfg = load_config(cfg_file)
        assert len(cfg.topics) == 1
        assert len(cfg.topics[0].subscriptions) == 1
        sub = cfg.topics[0].subscriptions[0]
        assert isinstance(sub, SubscriptionConfig)
        assert sub.name == "my-sub"
        assert sub.filter is None

    def test_load_config_object_with_filter(self, tmp_path):
        cfg_file = tmp_path / "config.yaml"
        cfg_file.write_text(
            "project_id: test\n"
            "topics:\n"
            "  - name: my-topic\n"
            "    subscriptions:\n"
            '      - name: my-sub\n'
            '        filter: \'attributes.service_name = "dps"\'\n'
        )
        cfg = load_config(cfg_file)
        sub = cfg.topics[0].subscriptions[0]
        assert sub.name == "my-sub"
        assert sub.filter == 'attributes.service_name = "dps"'

    def test_load_config_mixed_format(self, tmp_path):
        cfg_file = tmp_path / "config.yaml"
        cfg_file.write_text(
            "project_id: test\n"
            "topics:\n"
            "  - name: my-topic\n"
            "    subscriptions:\n"
            "      - plain-sub\n"
            '      - name: filtered-sub\n'
            '        filter: \'attributes.type = "requested"\'\n'
        )
        cfg = load_config(cfg_file)
        subs = cfg.topics[0].subscriptions
        assert len(subs) == 2
        assert subs[0].name == "plain-sub"
        assert subs[0].filter is None
        assert subs[1].name == "filtered-sub"
        assert subs[1].filter == 'attributes.type = "requested"'


# ── Broker integration tests ─────────────────────────────────────────


class TestBrokerFilter:
    def _make_broker(self, topic_name: str, subs: list[SubscriptionConfig]) -> MessageBroker:
        cfg = Config(
            project_id="test-project",
            topics=[TopicConfig(name=topic_name, subscriptions=subs)],
        )
        return MessageBroker(cfg)

    def test_filtered_delivery_only_matching_sub(self):
        broker = self._make_broker("ai-service-response", [
            SubscriptionConfig(name="dps-sub", filter='attributes.service_name = "dps"'),
            SubscriptionConfig(name="cms-sub", filter='attributes.service_name = "cms"'),
            SubscriptionConfig(name="ags-sub", filter='attributes.service_name = "ags"'),
        ])
        topic_path = broker.config.topic_path("ai-service-response")

        ids = asyncio.get_event_loop().run_until_complete(
            broker.publish(topic_path, [{"data": b"hello", "attributes": {"service_name": "dps"}}])
        )
        assert len(ids) == 1

        dps_sub = broker.subscriptions[broker.config.subscription_path("dps-sub")]
        cms_sub = broker.subscriptions[broker.config.subscription_path("cms-sub")]
        ags_sub = broker.subscriptions[broker.config.subscription_path("ags-sub")]

        assert len(dps_sub.pending) == 1
        assert len(cms_sub.pending) == 0
        assert len(ags_sub.pending) == 0

    def test_no_attributes_skips_filtered_subs(self):
        broker = self._make_broker("ai-service-response", [
            SubscriptionConfig(name="dps-sub", filter='attributes.service_name = "dps"'),
            SubscriptionConfig(name="cms-sub", filter='attributes.service_name = "cms"'),
        ])
        topic_path = broker.config.topic_path("ai-service-response")

        asyncio.get_event_loop().run_until_complete(
            broker.publish(topic_path, [{"data": b"hello", "attributes": {}}])
        )

        dps_sub = broker.subscriptions[broker.config.subscription_path("dps-sub")]
        cms_sub = broker.subscriptions[broker.config.subscription_path("cms-sub")]

        assert len(dps_sub.pending) == 0
        assert len(cms_sub.pending) == 0

    def test_unfiltered_sub_receives_everything(self):
        broker = self._make_broker("ai-service-request", [
            SubscriptionConfig(name="request-sub"),  # no filter
        ])
        topic_path = broker.config.topic_path("ai-service-request")

        asyncio.get_event_loop().run_until_complete(
            broker.publish(topic_path, [{"data": b"msg1", "attributes": {"service_name": "dps"}}])
        )
        asyncio.get_event_loop().run_until_complete(
            broker.publish(topic_path, [{"data": b"msg2", "attributes": {}}])
        )

        sub = broker.subscriptions[broker.config.subscription_path("request-sub")]
        assert len(sub.pending) == 2

    def test_mixed_filtered_and_unfiltered(self):
        broker = self._make_broker("mixed-topic", [
            SubscriptionConfig(name="all-sub"),  # no filter
            SubscriptionConfig(name="dps-only", filter='attributes.service_name = "dps"'),
        ])
        topic_path = broker.config.topic_path("mixed-topic")

        asyncio.get_event_loop().run_until_complete(
            broker.publish(topic_path, [{"data": b"hello", "attributes": {"service_name": "cms"}}])
        )

        all_sub = broker.subscriptions[broker.config.subscription_path("all-sub")]
        dps_sub = broker.subscriptions[broker.config.subscription_path("dps-only")]

        assert len(all_sub.pending) == 1
        assert len(dps_sub.pending) == 0
