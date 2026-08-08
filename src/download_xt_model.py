"""Download and register the fixed xT model."""

from __future__ import annotations

import hashlib
import json
from datetime import datetime, timezone
from pathlib import Path

import requests


MODEL_URL = (
    "https://karun.in/blog/data/"
    "open_xt_12x8_v1.json"
)

MODEL_VERSION = "open_xt_12x8_v1"

MODEL_DIR = Path("models/xt")

MODEL_PATH = (
    MODEL_DIR
    / "open_xt_12x8_v1.json"
)

METADATA_PATH = (
    MODEL_DIR
    / "metadata.json"
)


def main() -> None:

    response = requests.get(
        MODEL_URL,
        timeout=30,
    )

    response.raise_for_status()

    content = response.content

    # Make sure it is actually valid JSON.
    grid = json.loads(content)

    if len(grid) != 8:
        raise ValueError(
            "Expected xT model to contain 8 rows."
        )

    if any(len(row) != 12 for row in grid):
        raise ValueError(
            "Expected each xT row to contain 12 columns."
        )

    file_hash = hashlib.sha256(
        content
    ).hexdigest()

    MODEL_DIR.mkdir(
        parents=True,
        exist_ok=True,
    )

    MODEL_PATH.write_bytes(content)

    metadata = {
        "model_name": "Expected Threat",
        "model_version": MODEL_VERSION,
        "grid_width": 12,
        "grid_height": 8,
        "source_url": MODEL_URL,
        "sha256": file_hash,
        "downloaded_at": (
            datetime.now(timezone.utc)
            .isoformat()
        ),
    }

    METADATA_PATH.write_text(
        json.dumps(
            metadata,
            indent=2,
        ),
        encoding="utf-8",
    )

    print("xT model downloaded.")
    print(f"Version: {MODEL_VERSION}")
    print(f"SHA-256: {file_hash}")
    print(f"Path: {MODEL_PATH}")


if __name__ == "__main__":
    main()