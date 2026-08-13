"""Raw ingestion for StatsBomb match event data."""

from __future__ import annotations

import hashlib
import json
import logging
import uuid
from datetime import datetime, timezone

import requests

from src.config.settings import (
    HTTP_TIMEOUT_SECONDS,
    INGESTION_MANIFEST_PATH,
    PROVIDER,
    RAW_DIR,
    STATSBOMB_EVENTS_URL,
)
from src.storage.storage_store import (
    put_bytes,
)
from src.metadata.manifest import read_manifest


logger = logging.getLogger(__name__)

MANIFEST_PATH = INGESTION_MANIFEST_PATH
EVENTS_URL = STATSBOMB_EVENTS_URL


def calculate_sha256(content: bytes) -> str:
    """Calculate SHA-256 hash for raw source bytes."""
    return hashlib.sha256(content).hexdigest()


def find_existing_version(
    manifest: list[dict],
    match_id: int,
    file_hash: str,
) -> dict | None:
    """Return existing version if the same match/hash was ingested."""

    for record in manifest:
        if (
            record["match_id"] == match_id
            and record["file_hash"] == file_hash
        ):
            return record

    return None


def ingest_match(match_id: int) -> dict:
    """Download and register one StatsBomb match."""

    url = EVENTS_URL.format(match_id=match_id)

    response = requests.get(url, timeout=HTTP_TIMEOUT_SECONDS)
    response.raise_for_status()

    raw_content = response.content

    file_hash = calculate_sha256(raw_content)

    manifest = read_manifest()

    existing = find_existing_version(
        manifest=manifest,
        match_id=match_id,
        file_hash=file_hash,
    )

    if existing:
        logger.info(
            "match_already_ingested",
            extra={
                "match_id": match_id,
                "raw_path": existing["raw_path"],
                "file_hash": file_hash,
            },
        )
        return existing

    key = f"raw/match_id={match_id}/events_{file_hash[:12]}.json"
    raw_uri = put_bytes(
        key=key,
        bytes=raw_content,
    )

    previous_versions = [
        record
        for record in manifest
        if record["match_id"] == match_id
    ]

    source_version = len(previous_versions) + 1

    record = {
        "ingestion_id": str(uuid.uuid4()),
        "match_id": match_id,
        "provider": PROVIDER,
        "source_version": source_version,
        "source_url": url,
        "file_hash": file_hash,
        "raw_path": None,
        "raw_uri": raw_uri,
        "ingested_at": datetime.now(timezone.utc).isoformat(),
    }

    MANIFEST_PATH.parent.mkdir(
        parents=True,
        exist_ok=True,
    )

    with MANIFEST_PATH.open("a", encoding="utf-8") as file:
        file.write(json.dumps(record) + "\n")

    if previous_versions:
        logger.info(
            "source_changed_new_version_registered",
            extra={
                "match_id": match_id,
                "source_version": source_version,
                "raw_uri": raw_uri,
                "file_hash": file_hash,
            },
        )
    else:
        logger.info(
            "match_ingested",
            extra={
                "match_id": match_id,
                "source_version": source_version,
                "raw_uri": raw_uri,
                "file_hash": file_hash,
            },
        )

    return record


def main() -> None:
    import argparse

    parser = argparse.ArgumentParser()

    parser.add_argument(
        "--match-id",
        type=int,
        required=True,
    )

    args = parser.parse_args()

    ingest_match(args.match_id)


if __name__ == "__main__":
    main()