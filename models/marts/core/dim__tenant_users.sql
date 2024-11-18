{{
  config(
    materialized='table',
  )
}}

with

subscription_business as (

    select * from {{ ref('stg_prod__subscription_business') }}

),

subscription_user as (

    select * from {{ ref('stg_prod__subscription_user') }}

),

tenants as (

    select * from {{ ref('stg_prod__tenants') }}

),

users as (

    select * from {{ ref('stg_prod__users') }}

),

enriched as (

    select
        t.tenant_id,
        t.subscription_business_id,

        sb.subscription_id,
        sb.chargebee_subscription_id,

        su.user_id,
        u.user_identifier,
        su.name,

        sb.created_at as business_created_at,
        t.created_at as tenant_created_at,
        t.modified_at

    from subscription_business sb

    left join subscription_user su 

        on sb.subscription_id = su.subscription_id

    left join tenants t

        on sb.subscription_business_id = t.subscription_business_id

    left join users u

        on su.user_id = u.user_id

)

select * from enriched
