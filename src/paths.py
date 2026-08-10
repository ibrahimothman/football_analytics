"""Shared filesystem path helpers."""

from __future__ import annotations

from pathlib import Path


def latest_file(
    folder: Path,
    pattern: str,
) -> Path:
    """Return the most recently modified file matching a glob pattern."""

    files = list(folder.glob(pattern))

    if not files:
        raise FileNotFoundError(
            f"No files found in {folder} "
            f"matching '{pattern}'."
        )

    return max(
        files,
        key=lambda path: path.stat().st_mtime,
    )
