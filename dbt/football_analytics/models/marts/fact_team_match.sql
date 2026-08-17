{{
    config(
        materialized='incremental',
        unique_key=['match_id', 'team_id']
    )
}}

select
    match_id,
    team_id,
    team_name,
    goals,
    shots,
    xg,
    attempted_passes,
    completed_passes,
    progressive_passes,
    carries

from {{ ref('fact_gold_team') }}

{% if is_incremental() %}
where match_id = {{ var('match_id') }}
{% endif %}