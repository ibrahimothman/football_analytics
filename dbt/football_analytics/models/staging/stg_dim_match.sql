select 
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
    
from {{ source('football_analytics', 'dim_match') }}