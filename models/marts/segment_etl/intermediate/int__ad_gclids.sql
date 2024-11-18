{{
  config(
    materialized='incremental',
    unique_key=["last_gclid", "user_identifier", "event_at"]
  )
}}

with

pages as (

    select * from {{ ref('fct__pages') }}

),

identities as (

    select 

        anonymous_id,
        last_value(user_id) ignore nulls over (
            partition by anonymous_id order by event_at
        ) as last_user_id,

        row_number() over (partition by anonymous_id order by event_at desc) as rn
    
    from pages

),

pages_filtered as (

    select
        {{ get_query_param('search', 'gclid') }} as gclid,
        context_user_agent,

        coalesce(i.last_user_id, p.anonymous_id) as user_identifier,
        p.anonymous_id,
        
        event_at

    from pages p

    left join identities i

        on p.anonymous_id = i.anonymous_id

        and i.rn = 1

    where search like '%gclid%'

),

pages_marked as (

    select
        last_value(gclid) ignore nulls over (
            partition by user_identifier order by event_at
        ) as last_gclid,

        last_value(context_user_agent) ignore nulls over (
            partition by user_identifier order by event_at
        ) as last_context_user_agent,
    
        user_identifier,

        row_number() over (partition by user_identifier order by event_at desc) as rn,

        event_at

    from pages_filtered

),

pages_ids as (

    select * 
    
    from pages_marked

    where rn = 1

)

select * from pages_ids
