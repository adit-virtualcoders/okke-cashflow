{{
    config(
        materialized='incremental',
        unique_key="chargebee_subscription_id",
    )
}}

with

subs as (

    select * from {{ ref('stg_chargebee__subscriptions') }}

)

select * from subs
