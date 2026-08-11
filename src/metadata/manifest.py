"""Shared ingestion manifest helpers."""

from __future__ import annotations

import json
from pathlib import Path

from src.config.settings import INGESTION_MANIFEST_PATH


def read_manifest(
    *,
    require_exists: bool = False,
) -> list[dict]:
    """Read ingestion manifest records."""

    manifest_path = INGESTION_MANIFEST_PATH

    if not manifest_path.exists():
        if require_exists:
            raise FileNotFoundError(
                "Ingestion manifest does not exist. "
                "Ingest a match first."
            )
        return []

    records: list[dict] = []

    with manifest_path.open(
        "r",
        encoding="utf-8",
    ) as file:
        for line in file:
            if line.strip():
                records.append(json.loads(line))

    return records


def load_ingested_match_ids(
) -> set[int]:
    """Return match IDs already present in the ingestion manifest."""

    return {
        int(record["match_id"])
        for record in read_manifest()
    }
