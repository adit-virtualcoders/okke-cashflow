{{
    config(
        materialized='incremental',
        unique_key="event_id",
    )
}}

with

pages as (

    select * from {{ ref('int__pages_enriched') }}

),

pages_lagged as (

    select 
        *,
        lag(event_at, 1) over (partition by anonymous_id order by event_at) as last_visit_at

    from pages

),

pages_marked as (

    select 
        *,
        case 
            when datediff(second, last_visit_at, event_at) >= {{ var('secs_idle_time_sessions') }}
            or last_visit_at is null
        then 1 else 0 end as is_new_session

    from pages_lagged

),

pages_session_ids as (

    select 
        *,
        sum(is_new_session) over 
            (order by anonymous_id, event_at rows between unbounded preceding and current row) 
            as global_session_id,

        sum(is_new_session) over 
            (partition by anonymous_id order by event_at rows between unbounded preceding and current row) 
            as user_session_id

    from pages_marked

),

pages_session_timed as (
    
    select
        *,
        
        count(event_at) over (partition by global_session_id) as page_count,

        (
            datediff(s, 
                (min(event_at) over (partition by global_session_id)),
                (max(event_at) over (partition by global_session_id))
            )
        ) as session_secs

    from pages_session_ids

),

pages_to_sessions as (

    select 
        p.*

    from pages_session_timed p

    where p.is_new_session = 1

)

select * from pages_to_sessions
