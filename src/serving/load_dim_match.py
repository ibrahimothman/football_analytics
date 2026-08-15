import pandas as pd

from src.config.settings import (
    SERVING_DIR as SETTINGS_SERVING_DIR,
)

from src.storage.database import (
    get_db_connection,
)

UPSERT_DIM_MATCH = """
INSERT INTO serving.dim_match (
    match_id,
    match_date,
    kickoff_time,
    competition_id,
    competition_name,
    season_id,
    season_name,
    match_week,
    home_team_id,
    home_team_name,
    away_team_id,
    away_team_name,
    stadium,
    referee
)
VALUES (
    %s, %s, %s, %s, %s, %s, %s,
    %s, %s, %s, %s, %s, %s, %s
)

ON CONFLICT (match_id)
DO UPDATE SET
    match_date = EXCLUDED.match_date,
    kickoff_time = EXCLUDED.kickoff_time,
    competition_id = EXCLUDED.competition_id,
    competition_name = EXCLUDED.competition_name,
    season_id = EXCLUDED.season_id,
    season_name = EXCLUDED.season_name,
    match_week = EXCLUDED.match_week,
    home_team_id = EXCLUDED.home_team_id,
    home_team_name = EXCLUDED.home_team_name,
    away_team_id = EXCLUDED.away_team_id,
    away_team_name = EXCLUDED.away_team_name,
    stadium = EXCLUDED.stadium,
    referee = EXCLUDED.referee;
"""

def upsert_dim_match(dim_matches_df: pd.DataFrame):
    rows = [
        (
            row["match_id"],
            row["match_date"],
            row["kickoff_time"],
            row["competition_id"],
            row["competition_name"],
            row["season_id"],
            row["season_name"],
            row["match_week"],
            row["home_team_id"],
            row["home_team_name"],
            row["away_team_id"],
            row["away_team_name"],
            row["stadium"],
            row["referee"],
        )
        for _, row in dim_matches_df.iterrows()
    ]

    with get_db_connection() as conn:
        with conn.cursor() as cursor:
            cursor.executemany(UPSERT_DIM_MATCH, rows)
        conn.commit()
