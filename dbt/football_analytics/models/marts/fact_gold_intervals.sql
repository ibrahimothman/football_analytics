{{ config(
    materialized='incremental',
    unique_key=['match_id', 'team_id', 'period', 'interval_start', 'is_stoppage_time'],
) }}

-- can we create a view, also xg where is_shot can be forgot
with events as (
    select
        *
    from {{ ref('stg_silver_events') }}
    where team_id is not null
        and period in (1, 2)
    {% if is_incremental() %}
    and match_id = {{ var('match_id') }}
    {% endif %}
),

bucketed as (
    select
        *,
        case 
            when period = 1 then minute
            else minute - 45
        end as period_minute,

        (period = 1 and minute >= 45) 
            or (period = 2 and minute >= 90) as is_stoppage_time

    from events
),

assigned as (
    select
        *,

        case 
            when is_stoppage_time then 45
            else (period_minute / 5) * 5
        end as interval_start

    from bucketed
),

aggregated as (
    select
        team_id,
        match_id,
        period,
        interval_start,
        is_stoppage_time,
        

        coalesce(sum(xt_added) filter (
            where is_successful_move and xt_added > 0
        ), 0) as positive_xt,

        coalesce(sum(xt_added) filter (
            where is_successful_move and xt_added < 0
        ), 0) as negative_xt,

        coalesce(sum(xt_added) filter (
            where is_successful_move
        ), 0) as net_xt,


         count(*) filter (
            where is_successful_move
        ) as successful_moves,

        count(*) filter (
            where is_shot
        ) as shots,

        coalesce(sum(xt_added) filter (
            where is_shot
        ), 0) as xg

    from assigned
    group by 
        team_id,
        match_id,
        period,
        interval_start,
        is_stoppage_time
),


teams as (
    select
        match_id,
        team_id,
        max(team_name) as team_name,
        max(source_version) as source_version,
        max(file_hash) as file_hash,
        max(xt_model_version) as xt_model_version

    from events
    group by match_id, team_id
),

periods as (
    select 1 as period
    union all
    select 2
),

slots as (
    select generate_series(0, 40, 5) as interval_start,
        false as is_stoppage_time
    union all
    select 45, true
),

spine as (
    select
        t.match_id,
        t.team_id,
        t.team_name,
        p.period,
        s.interval_start,
        s.is_stoppage_time,
        t.source_version,
        t.file_hash,
        t.xt_model_version,
        '1.0' as metric_version
    from teams t
    cross join periods p
    cross join slots s

),

labeled as (
    select
        *,
        case 
            when period = 1 and is_stoppage_time then '45+'
            when period = 2 and is_stoppage_time then '90+'
            when period = 1
                then interval_start::text 
                    || '-' 
                    || (interval_start + 5)::text
            when period = 2
                then (45 + interval_start)::text 
                    || '-' 
                    || (50 + interval_start)::text
        end as interval_label
    from spine
)

select 
    l.match_id,
    l.team_id,
    l.team_name,
    l.period,
    l.interval_start,
    l.is_stoppage_time,
    l.interval_label,

    coalesce(a.positive_xt, 0) as positive_xt,
    coalesce(a.negative_xt, 0) as negative_xt,
    coalesce(a.net_xt, 0) as net_xt,
    coalesce(a.successful_moves, 0) as successful_moves,
    coalesce(a.shots, 0) as shots,
    coalesce(a.xg, 0) as xg,

    l.source_version,
    l.file_hash,
    l.xt_model_version,
    l.metric_version
    
    from labeled l
    left join aggregated a 
        on l.match_id = a.match_id 
        and l.team_id = a.team_id 
        and l.period = a.period 
        and l.interval_start = a.interval_start 
        and l.is_stoppage_time = a.is_stoppage_time