{{
  config(
    materialized='incremental',
    unique_key="tenant_id"
  )
}}

with 

tenants as (

    select * from {{ ref('stg_prod__tenants') }}

),

subscription_business as (

    select * from {{ ref('stg_prod__subscription_business') }}

),

enriched as (

    select
        
        sb.chargebee_subscription_id,
        t.*

    from tenants t

    left join subscription_business sb

        on t.subscription_business_id = sb.subscription_business_id

)

select * from enriched
