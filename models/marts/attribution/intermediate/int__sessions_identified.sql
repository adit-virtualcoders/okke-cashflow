{{
  config(
    materialized='incremental',
    unique_key="event_id"
  )
}}

with

sessions_raw as (

    select * from {{ ref('dim__sessions') }}

),

identities as (

    select * from {{ ref('fct__identities') }}

),

sessions_identified as (

    select
        coalesce(s.user_id, i.user_id, s.anonymous_id) as user_identifier,
        s.*

    from sessions_raw s

    left join identities i

    on s.anonymous_id = i.anonymous_id

)

select * from sessions_identified
