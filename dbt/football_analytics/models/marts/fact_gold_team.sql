{{ config(
    materialized='incremental',
    unique_key=['match_id', 'team_id'],
) }}

with events as (
    select 
        *
    from {{ ref('stg_silver_events') }}
    where team_id is not null
    {% if is_incremental() %}
    and match_id = {{ var('match_id') }}
    {% endif %}
),

aggregated as (
    select 
        match_id,
        team_id,
        max(team_name) as team_name,

        sum(is_shot::int) as shots,
        sum((is_shot and outcome = 'Goal')::int) as goals,
        sum(coalesce(shot_xg, 0)) filter (where is_shot) as xg,

        sum(is_pass::int) as attempted_passes,
        sum(is_completed_pass::int) as completed_passes,
        sum(is_progressive_pass::int) as progressive_passes,
        sum(is_carry::int) as carries,

        max(source_version) as source_version,
        max(file_hash) as file_hash,
        '1.0' as metric_version

    from events
    group by 
        match_id,
        team_id
)

select 
    match_id,
    team_id,
    team_name,
    shots,
    goals,
    xg,
    attempted_passes,
    completed_passes,
    progressive_passes,
    carries,

    CASE 
        WHEN attempted_passes > 0 
        THEN completed_passes / attempted_passes * 100
    END as pass_completion_pct,

    source_version,
    file_hash,
    metric_version

from aggregated