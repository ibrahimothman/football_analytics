"""DuckDB views over pipeline observability metadata."""

from __future__ import annotations

from pathlib import Path

import duckdb

from src.config.settings import (
    AIRFLOW_EVENTS_PATH,
    INGESTION_MANIFEST_PATH,
    METADATA_DIR,
)


AIRFLOW_PIPELINE_STAGE_RUNS_PATH = (
    METADATA_DIR / "airflow_pipeline_stage_runs.jsonl"
)


def _sql_path(path: Path) -> str:
    """Quote a filesystem path for use inside DuckDB SQL."""

    return str(path).replace("\\", "/").replace("'", "''")


def create_observability_views(
    con: duckdb.DuckDBPyConnection | None = None,
) -> duckdb.DuckDBPyConnection:
    """Create observability schema views over metadata JSONL files."""

    if con is None:
        con = duckdb.connect()

    stage_runs_path = _sql_path(
        AIRFLOW_PIPELINE_STAGE_RUNS_PATH
    )
    events_path = _sql_path(AIRFLOW_EVENTS_PATH)
    manifest_path = _sql_path(INGESTION_MANIFEST_PATH)

    con.execute(
        "CREATE SCHEMA IF NOT EXISTS observability"
    )

    con.execute(
        f"""
        CREATE OR REPLACE VIEW
            observability.airflow_pipeline_stage_runs AS
        SELECT *
        FROM read_json_auto(
            '{stage_runs_path}',
            format = 'newline_delimited'
        )
        """
    )

    con.execute(
        f"""
        CREATE OR REPLACE VIEW
            observability.airflow_events AS
        SELECT *
        FROM read_json_auto(
            '{events_path}',
            format = 'newline_delimited'
        )
        """
    )

    con.execute(
        f"""
        CREATE OR REPLACE VIEW
            observability.ingestion_manifest AS
        SELECT *
        FROM read_json_auto(
            '{manifest_path}',
            format = 'newline_delimited'
        )
        """
    )

    con.execute(
        """
        CREATE OR REPLACE VIEW
            observability.stage_health AS
        SELECT
            stage,

            COUNT(*) AS attempts,

            COUNT(*) FILTER (
                WHERE status = 'SUCCEEDED'
            ) AS successful_attempts,

            COUNT(*) FILTER (
                WHERE status = 'FAILED'
            ) AS failed_attempts,

            AVG(duration_seconds)
                FILTER (
                    WHERE status = 'SUCCEEDED'
                )
                AS avg_duration_seconds,

            MAX(duration_seconds)
                FILTER (
                    WHERE status = 'SUCCEEDED'
                )
                AS max_duration_seconds,

            AVG(rows_out)
                FILTER (
                    WHERE status = 'SUCCEEDED'
                      AND rows_out IS NOT NULL
                )
                AS avg_rows_out

        FROM observability.airflow_pipeline_stage_runs

        GROUP BY stage
        """
    )

    con.execute(
        """
        CREATE OR REPLACE VIEW
            observability.stage_volume_history AS
        SELECT
            match_id,
            stage,
            completed_at,
            rows_in,
            rows_out
        FROM observability.airflow_pipeline_stage_runs
        WHERE status = 'SUCCEEDED'
          AND rows_out IS NOT NULL
        """
    )


    con.execute(
        """
        CREATE OR REPLACE VIEW
            observability.silver_volume_monitor AS
        
        WITH silver_runs AS (
            SELECT
                match_id,
                completed_at,
                rows_out,

                MEDIAN(rows_out) OVER (
                    ORDER BY completed_at

                    ROWS BETWEEN
                        10 PRECEDING 
                        AND 1 PRECEDING
                ) AS baseline_rows,

                COUNT(*) OVER (
                    ORDER BY completed_at

                    ROWS BETWEEN
                        10 PRECEDING 
                        AND 1 PRECEDING
                ) AS baseline_sample

            FROM observability.stage_volume_history
            WHERE stage = 'SILVER'
        )

        SELECT
            match_id,
            completed_at,
            rows_out,
            baseline_rows,
            
            rows_out / NULLIF(baseline_rows, 0) AS deviation_ratio,

            CASE
                WHEN baseline_sample < 10 THEN 'INSUFFICIENT_HISTORY'
                WHEN deviation_ratio > 1.50 THEN 'HIGH_VOLUME'
                WHEN deviation_ratio < 0.5 THEN 'LOW_VOLUME'
                ELSE 'NORMAL'
            END AS volume_status

        FROM silver_runs
        """
    )

    return con


def get_connection() -> duckdb.DuckDBPyConnection:
    """Return a DuckDB connection with observability views registered."""

    return create_observability_views()
