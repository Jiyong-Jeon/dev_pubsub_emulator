from __future__ import annotations

import logging
import re
from typing import Callable

logger = logging.getLogger(__name__)

_ATTR_EQ_RE = re.compile(r'^attributes\.(\w+)\s*=\s*"([^"]*)"$')


def parse_filter(expr: str | None) -> Callable[[dict[str, str]], bool]:
    """Parse a GCP Pub/Sub subscription filter expression.

    Supports: attributes.KEY = "VALUE" (equality only).
    Returns a predicate that checks message attributes.
    """
    if not expr:
        return lambda attrs: True

    m = _ATTR_EQ_RE.match(expr.strip())
    if not m:
        logger.warning("Unsupported filter expression (pass-through): %s", expr)
        return lambda attrs: True

    key, value = m.group(1), m.group(2)
    return lambda attrs, _k=key, _v=value: attrs.get(_k) == _v
