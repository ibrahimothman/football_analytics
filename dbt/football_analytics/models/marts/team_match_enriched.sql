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

FROM {{ ref('fact_gold_team') }} AS f
JOIN {{ ref('stg_dim_match') }} AS m
    ON f.match_id = m.match_id