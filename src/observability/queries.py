"""Query helpers over observability DuckDB views."""

from __future__ import annotations

import pandas as pd

from src.observability.duckdb_views import get_connection


def stage_health() -> pd.DataFrame:
    """Return per-stage attempt / success / duration / rows summary."""

    con = get_connection()

    query = """
    SELECT
        stage,
        attempts,
        successful_attempts,
        failed_attempts,
        avg_duration_seconds,
        max_duration_seconds,
        avg_rows_out
    FROM observability.stage_health
    ORDER BY stage
    """

    return con.execute(query).df()


def stage_volume_history(
    stage: str | None = None,
) -> pd.DataFrame:
    """Return successful stage volume history, optionally filtered by stage."""

    con = get_connection()

    if stage is None:
        query = """
        SELECT
            match_id,
            stage,
            completed_at,
            rows_in,
            rows_out
        FROM observability.stage_volume_history
        ORDER BY completed_at, match_id, stage
        """
        return con.execute(query).df()

    query = """
    SELECT
        match_id,
        stage,
        completed_at,
        rows_in,
        rows_out
    FROM observability.stage_volume_history
    WHERE stage = ?
    ORDER BY completed_at, match_id
    """
    return con.execute(query, [stage]).df()


def ingestion_freshness() -> pd.DataFrame:
    """Return ingestion freshness summary."""

    con = get_connection()

    query = """
    SELECT
        Max(ingested_at) 
            AS latest_ingested_at
    FROM observability.ingestion_freshness
    """
