with silver as (
    select
        match_id,
        team_id,
        sum(is_shot::int) as shots,
        sum(coalesce(shot_xg, 0)) filter (where is_shot) as xg,
        sum(is_completed_pass::int) as completed_passes
    from {{ ref('stg_silver_events') }}
    where team_id is not null
    group by match_id, team_id
)

select 
    g.match_id,
    g.team_id,
    g.shots as gold_shots,
    s.shots as silver_shots,
    g.completed_passes as gold_completed_passes,
    s.completed_passes as silver_completed_passes,
    g.xg as gold_xg,
    s.xg as silver_xg

from {{ ref('fact_gold_team') }} as g
join silver as s
    on g.match_id = s.match_id
    and g.team_id = s.team_id

where g.shots != s.shots
    or g.completed_passes != s.completed_passes
    or abs(g.xg - s.xg) > 1e-6
