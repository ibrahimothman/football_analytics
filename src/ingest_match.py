"""Raw ingestion for StatsBomb match event data."""

from __future__ import annotations

import hashlib
import json
import uuid
from datetime import datetime, timezone
from pathlib import Path

import requests


PROVIDER = "statsbomb_open_data"

EVENTS_URL = (
    "https://raw.githubusercontent.com/"
    "statsbomb/open-data/master/data/events/{match_id}.json"
)

RAW_DIR = Path("data/raw")
MANIFEST_PATH = Path("data/metadata/ingestion_manifest.jsonl")


def calculate_sha256(content: bytes) -> str:
    """Calculate SHA-256 hash for raw source bytes."""
    return hashlib.sha256(content).hexdigest()


def read_manifest() -> list[dict]:
    """Read existing source versions from the manifest."""

    if not MANIFEST_PATH.exists():
        return []

    records = []

    with MANIFEST_PATH.open("r", encoding="utf-8") as file:
        for line in file:
            if line.strip():
                records.append(json.loads(line))

    return records


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

    print(f"Fetching match {match_id}...")

    response = requests.get(url, timeout=30)
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
        print("Exact source version already ingested.")
        print(f"Raw file: {existing['raw_path']}")

        return existing

    previous_versions = [
        record
        for record in manifest
        if record["match_id"] == match_id
    ]

    source_version = len(previous_versions) + 1

    match_directory = RAW_DIR / f"match_id={match_id}"
    match_directory.mkdir(parents=True, exist_ok=True)

    filename = f"events_{file_hash[:12]}.json"
    raw_path = match_directory / filename

    raw_path.write_bytes(raw_content)

    record = {
        "ingestion_id": str(uuid.uuid4()),
        "match_id": match_id,
        "provider": PROVIDER,
        "source_version": source_version,
        "source_url": url,
        "file_hash": file_hash,
        "raw_path": str(raw_path),
        "ingested_at": datetime.now(timezone.utc).isoformat(),
    }

    MANIFEST_PATH.parent.mkdir(
        parents=True,
        exist_ok=True,
    )

    with MANIFEST_PATH.open("a", encoding="utf-8") as file:
        file.write(json.dumps(record) + "\n")

    if previous_versions:
        print(
            f"Source changed. Registered version "
            f"{source_version}."
        )
    else:
        print("New match successfully ingested.")

    print(f"Raw file: {raw_path}")
    print(f"SHA-256: {file_hash}")

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