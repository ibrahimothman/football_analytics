import pandas as pd

from src.storage.database import (
    get_db_connection,
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