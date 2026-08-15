import pandas as pd

from src.storage.database import (
    get_db_connection,
)

UPSERT_FACT_TEAM_MATCH = """
INSERT INTO serving.fact_team_match (
    match_id,
    team_id,
    team_name,
    goals,
    shots,
    xg,
    passes_attempted,
    passes_completed,
    progressive_passes,
    carries
)
VALUES (
    %s, %s, %s, %s, %s,
    %s, %s, %s, %s, %s
)

ON CONFLICT (match_id, team_id)
DO UPDATE SET
    team_name = EXCLUDED.team_name,
    goals = EXCLUDED.goals,
    shots = EXCLUDED.shots,
    xg = EXCLUDED.xg,
    passes_attempted = EXCLUDED.passes_attempted,
    passes_completed = EXCLUDED.passes_completed,
    progressive_passes = EXCLUDED.progressive_passes,
    carries = EXCLUDED.carries
"""

def upsert_fact_team_match(fact_team_matches_df: pd.DataFrame):
    rows = [
        (
            row["match_id"],
            row["team_id"],
            row["team_name"],
            row["goals"],
            row["shots"],
            row["xg"],
            row["passes_attempted"],
            row["passes_completed"],
            row["progressive_passes"],
            row["carries"],
        )
        for _, row in fact_team_matches_df.iterrows()
    ]

    with get_db_connection() as conn:
        with conn.cursor() as cursor:
            cursor.executemany(UPSERT_FACT_TEAM_MATCH, rows)
        conn.commit()