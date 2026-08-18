import pandas as pd

from src.storage.database import (
    get_db_connection,
)
from src.storage.iceberg import load_table
from openlineage.client.event_v2 import (
    InputDataset,
    OutputDataset,
)
from airflow.providers.openlineage.api.datasets import (
    emit_dataset_lineage,
)

copy_sql = """
    COPY serving.src_silver_events (
        match_id, event_id, event_index, period, minute, second,
        event_type, team_id, team_name, player_id, player_name,
        possession_id, start_x, start_y, end_x, end_y, outcome,
        shot_xg, is_pass, is_carry, is_shot, is_completed_pass,
        is_progressive_pass, progress_ratio, progress_toward_goal_m,
        is_successful_move, xt_start, xt_end, xt_added,
        xt_model_version, source_version, file_hash
    ) FROM STDIN
"""

COLUMNS = [
    "match_id",
    "event_id",
    "event_index",
    "period",
    "minute",
    "second",
    "event_type",
    "team_id",
    "team_name",
    "player_id",
    "player_name",
    "possession_id",
    "start_x",
    "start_y",
    "end_x",
    "end_y",
    "outcome",
    "shot_xg",
    "is_pass",
    "is_carry",
    "is_shot",
    "is_completed_pass",
    "is_progressive_pass",
    "progress_ratio",
    "progress_toward_goal_m",
    "is_successful_move",
    "xt_start",
    "xt_end",
    "xt_added",
    "xt_model_version",
    "source_version",
    "file_hash",
]


def _to_db(value):
    if pd.isna(value):
        return None
    return value


def load_silver_to_postgres(match_id: int, snapshot_id: int):
    table = load_table(
        table_name="football.silver_events",
    )

    silver_arrow_table = table.scan(
        snapshot_id=snapshot_id,
        row_filter=f"match_id = {match_id}",
    ).to_arrow()

    silver_df = silver_arrow_table.to_pandas(types_mapper=pd.ArrowDtype)

    upsert_src_silver_events(silver_df)

    emit_dataset_lineage(
        inputs=[
            InputDataset(
                namespace="iceberg",
                name="football.silver_events",
                facets={},
                inputFacets={},
            )
        ],
        outputs=[
            OutputDataset(
                namespace="postgres://postgres:5432",
                name="football.serving.src_silver_events",
                facets={},
                outputFacets={},
            )
        ],
    )

def upsert_src_silver_events(silver_df: pd.DataFrame):
    match_id = silver_df["match_id"].iloc[0]

    with get_db_connection() as conn:
        with conn.cursor() as cursor:
            cursor.execute(
                "DELETE FROM serving.src_silver_events WHERE match_id = %s",
                (match_id,)
            )
            with cursor.copy(copy_sql) as copy:
                for row in silver_df[COLUMNS].itertuples(index=False, name=None):
                    copy.write_row(tuple(_to_db(v) for v in row))