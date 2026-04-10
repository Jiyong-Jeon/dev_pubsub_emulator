from __future__ import annotations

import argparse
import json
import sys
import urllib.error
import urllib.request


DEFAULT_HOST = "localhost"
DEFAULT_PORT = 8086


def _url(host: str, port: int, path: str) -> str:
    return f"http://{host}:{port}{path}"


def _get(host: str, port: int, path: str) -> dict | list:
    req = urllib.request.Request(_url(host, port, path))
    with urllib.request.urlopen(req) as resp:
        return json.loads(resp.read())


def _post(host: str, port: int, path: str, body: dict) -> dict:
    data = json.dumps(body).encode()
    req = urllib.request.Request(
        _url(host, port, path),
        data=data,
        headers={"Content-Type": "application/json"},
        method="POST",
    )
    with urllib.request.urlopen(req) as resp:
        return json.loads(resp.read())


def _short(path: str) -> str:
    return path.rsplit("/", 1)[-1]


# ── Commands ────────────────────────────────────────────────────────


def cmd_publish(args: argparse.Namespace) -> None:
    body: dict = {"data": args.data, "attributes": {}}
    if args.attrs:
        try:
            body["attributes"] = json.loads(args.attrs)
        except json.JSONDecodeError:
            print("Error: --attrs must be valid JSON", file=sys.stderr)
            sys.exit(1)

    result = _post(args.host, args.port, f"/api/publish/{args.topic}", body)
    print(f"Published → message_ids: {result['message_ids']}")


def cmd_topics(args: argparse.Namespace) -> None:
    topics = _get(args.host, args.port, "/api/topics")
    if not topics:
        print("No topics.")
        return
    for t in topics:
        subs = ", ".join(_short(s) for s in t.get("subscriptions", []))
        print(f"  {_short(t['name']):<30} subs: [{subs}]")


def cmd_subs(args: argparse.Namespace) -> None:
    subs = _get(args.host, args.port, "/api/subscriptions")
    if not subs:
        print("No subscriptions.")
        return
    for s in subs:
        parts = [
            f"topic={_short(s['topic'])}",
            f"pending={s['pending']}",
            f"outstanding={s['outstanding']}",
            f"streams={s['streams']}",
        ]
        if s.get("filter"):
            parts.append(f"filter={s['filter']}")
        print(f"  {_short(s['name']):<35} {', '.join(parts)}")


def cmd_stats(args: argparse.Namespace) -> None:
    stats = _get(args.host, args.port, "/api/stats")
    for k, v in stats.items():
        print(f"  {k:<15} {v}")


def cmd_messages(args: argparse.Namespace) -> None:
    msgs = _get(args.host, args.port, "/api/messages")
    msgs = list(reversed(msgs))  # newest first
    if args.limit:
        msgs = msgs[: args.limit]
    if not msgs:
        print("No messages.")
        return
    for m in msgs:
        ack = "ACK" if m.get("acked") else "UNACK"
        attrs = m.get("attributes", {})
        attrs_str = f" attrs={attrs}" if attrs else ""
        data_preview = (m.get("data") or "")[:80]
        print(f"  [{ack:<5}] {m['message_id']}  {_short(m['topic']):<25} {data_preview}{attrs_str}")


# ── Entry point ─────────────────────────────────────────────────────


def main() -> None:
    parser = argparse.ArgumentParser(
        prog="dev-pubsub-cli",
        description="CLI client for Dev PubSub Emulator",
    )
    parser.add_argument("--host", default=DEFAULT_HOST, help="Emulator host (default: localhost)")
    parser.add_argument("--port", type=int, default=DEFAULT_PORT, help="Emulator web port (default: 8086)")

    sub = parser.add_subparsers(dest="command", required=True)

    # publish
    p = sub.add_parser("publish", aliases=["pub"], help="Publish a message to a topic")
    p.add_argument("topic", help="Topic name")
    p.add_argument("--data", "-d", default="", help="Message data")
    p.add_argument("--attrs", "-a", help='Attributes as JSON (e.g. \'{"key":"value"}\')')
    p.set_defaults(func=cmd_publish)

    # topics
    p = sub.add_parser("topics", help="List topics")
    p.set_defaults(func=cmd_topics)

    # subs
    p = sub.add_parser("subs", help="List subscriptions")
    p.set_defaults(func=cmd_subs)

    # stats
    p = sub.add_parser("stats", help="Show stats")
    p.set_defaults(func=cmd_stats)

    # messages
    p = sub.add_parser("messages", aliases=["msgs"], help="Show recent messages")
    p.add_argument("--limit", "-n", type=int, default=20, help="Max messages to show (default: 20)")
    p.set_defaults(func=cmd_messages)

    args = parser.parse_args()

    try:
        args.func(args)
    except urllib.error.URLError as e:
        print(f"Error: Cannot connect to emulator at {args.host}:{args.port} ({e.reason})", file=sys.stderr)
        sys.exit(1)


if __name__ == "__main__":
    main()
