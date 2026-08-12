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

    con.execute(
        """
        CREATE OR REPLACE VIEW
            observability.recent_stage_health AS

        WITH ranked AS (
            SELECT
                *,
                ROW_NUMBER() OVER (
                    PARTITION BY stage
                    ORDER BY completed_at DESC
                ) AS rank
            FROM observability.airflow_pipeline_stage_runs
        )

        SELECT
            stage,
            match_id,
            airflow_run_id,
            task_id,
            try_number,
            status,
            started_at,
            completed_at,
            duration_seconds,
            rows_in,
            rows_out,
            CASE
                WHEN status = 'FAILED' THEN 'CRITICAL'
                WHEN status = 'SUCCEEDED'
                     AND try_number > 1 THEN 'WARNING'
                ELSE 'HEALTHY'
            END AS health_status
        FROM ranked
        WHERE rank = 1
        """
    )

    con.execute(
        """
        CREATE OR REPLACE VIEW
            observability.silver_volume_health AS
        SELECT
            match_id,
            completed_at,
            rows_out,
            baseline_rows,
            deviation_ratio,
            volume_status,
            CASE
                WHEN volume_status IN (
                    'HIGH_VOLUME',
                    'LOW_VOLUME'
                ) THEN 'WARNING'
                WHEN volume_status = 'INSUFFICIENT_HISTORY'
                    THEN 'INFO'
                ELSE 'HEALTHY'
            END AS health_status
        FROM observability.silver_volume_monitor
        """
    )

    con.execute(
        """
        CREATE OR REPLACE VIEW
            observability.alert_candidates AS

        SELECT
            stage AS component,
            match_id,
            completed_at AS occurred_at,
            health_status,
            status AS reason

        FROM observability.recent_stage_health

        WHERE health_status = 'CRITICAL'

        UNION ALL

        SELECT
            'SILVER_VOLUME' AS component,
            match_id,
            completed_at AS occurred_at,
            health_status,
            volume_status AS reason

        FROM observability.silver_volume_health
        
        WHERE health_status = 'WARNING'
        """
    )

    con.execute(
        """
        CREATE OR REPLACE VIEW
            observability.current_health AS

        WITH signals AS (
            SELECT
                stage AS component,
                health_status
            FROM observability.recent_stage_health

            UNION ALL

            SELECT
                'SILVER_VOLUME' AS component,
                health_status
            FROM (
                SELECT
                    health_status,
                    ROW_NUMBER() OVER (
                        ORDER BY completed_at DESC
                    ) AS rn
                FROM observability.silver_volume_health
            )
            WHERE rn = 1
        ),

        scored AS (
            SELECT
                component,
                health_status,
                CASE health_status
                    WHEN 'CRITICAL' THEN 3
                    WHEN 'WARNING' THEN 2
                    WHEN 'INFO' THEN 1
                    ELSE 0
                END AS severity
            FROM signals
        )

        SELECT
            CASE MAX(severity)
                WHEN 3 THEN 'CRITICAL'
                WHEN 2 THEN 'WARNING'
                WHEN 1 THEN 'INFO'
                ELSE 'HEALTHY'
            END AS overall_health,

            COUNT(*) FILTER (
                WHERE severity = 3
            ) AS critical_signals,

            COUNT(*) FILTER (
                WHERE severity = 2
            ) AS warning_signals,

            COUNT(*) FILTER (
                WHERE severity = 1
            ) AS info_signals
        FROM scored
        """
    )

    con.execute(
        """
        CREATE OR REPLACE VIEW
            observability.retry_summary AS
        SELECT
            airflow_run_id,
            task_id,
            match_id,

            COUNT(*) AS retry_counts,

            MIN(occurred_at) AS first_retry_at,
            MAX(occurred_at) AS last_retry_at

        FROM observability.airflow_events

        WHERE event_type = 'TASK_RETRY'

        GROUP BY
            airflow_run_id,
            task_id,
            match_id
        """
    )

    return con


def get_connection() -> duckdb.DuckDBPyConnection:
    """Return a DuckDB connection with observability views registered."""

    return create_observability_views()
