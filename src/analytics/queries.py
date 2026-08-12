import duckdb

from src.config.settings import SERVING_DIR

def get_connection():
    con = duckdb.connect()

    fact_team_match_path = str(SERVING_DIR / "fact_team_match.parquet")
    dim_match_path = str(SERVING_DIR / "dim_match.parquet")

    con.execute(
        f"""
        CREATE OR REPLACE VIEW fact_team_match AS
        SELECT *
        FROM read_parquet('{fact_team_match_path}')
        """)

    con.execute(
        f"""
        CREATE OR REPLACE VIEW dim_match AS
        SELECT *
        FROM read_parquet('{dim_match_path}')
        """)

    con.execute(f"""
        CREATE OR REPLACE VIEW team_match_enriched AS
        SELECT
            f.*,
            m.match_date,
            m.season_name,
            m.home_team_id,
            m.home_team_name,
            m.away_team_id,
            m.away_team_name,

            CASE
                WHEN f.team_id = m.home_team_id THEN 'Home'
                ELSE 'Away'
            END AS venue,

            CASE
                WHEN f.team_id = m.home_team_id
                    THEN m.away_team_name
                ELSE m.home_team_name
            END AS opponent

        FROM fact_team_match AS f
        JOIN dim_match AS m
            ON f.match_id = m.match_id
        """)

    return con


def team_season_summary(
    team_name: str,
    season_name: str,
):

    con = get_connection()
    query = f"""
    SELECT 
        team_name,
        season_name,

        count(*) as matches,
        sum(goals) as goals,
        sum(shots) as shots,
        sum(xg) as xg,
        sum(progressive_passes) as progressive_passes,

    FROM team_match_enriched
    WHERE 
        team_name = ? AND season_name = ?

    GROUP BY 
        team_name, 
        season_name
    """

    return con.execute(query, (team_name, season_name)).df()

def match_by_match_performance(
    team_name: str,
    season_name: str,
):

    con = get_connection()

    query = f"""
    SELECT 
        match_date,
        opponent,
        venue,
        goals,
        xg

    FROM team_match_enriched
    WHERE 
        team_name = ? AND season_name = ?
    ORDER BY 
        match_date
    """

    return con.execute(query, (team_name, season_name)).df()


def team_home_away_summary(
    team_name: str,
    season_name: str,
):    

    con = get_connection()

    query = f"""
    SELECT 
        venue,

        count(*) as matches,
        sum(goals) as goals,
        sum(xg) as xg,
        avg(goals) as goals_per_match,
        avg(xg) as xg_per_match,

    FROM team_match_enriched

    WHERE 
        team_name = ? AND season_name = ?

    GROUP BY 
        venue
        
    ORDER BY 
        venue
    """

    return con.execute(query, (team_name, season_name)).df()
