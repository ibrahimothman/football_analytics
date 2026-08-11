"""Shared helpers for nested JSON extraction."""

from __future__ import annotations

from typing import Any


def nested_value(
    data: dict[str, Any],
    parent: str,
    child: str,
) -> Any:
    """Safely extract a value from a nested object."""

    parent_value = data.get(parent)

    if not isinstance(parent_value, dict):
        return None

    return parent_value.get(child)
