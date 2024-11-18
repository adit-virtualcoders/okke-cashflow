{{
  config(
    materialized='incremental',
    unique_key="chargebee_subscription_id"
  )
}}

with

subs as (

    select * from {{ ref('dim__chargebee_subscriptions') }}

),

invoices as (

    select * from {{ ref('dim__chargebee_invoices') }}

),

invoices_agged as (

    select
        chargebee_subscription_id,
        sum(amount_paid) as total_amount_paid

    from invoices

    group by chargebee_subscription_id

),

subs_filter_tests as (

    select *

    from subs

    where {{ dedupe_filter('email') }}

),

subs_enriched as (

    select
        s.*,
        ia.total_amount_paid

    from subs_filter_tests s

    left join invoices_agged ia

        on s.chargebee_subscription_id = ia.chargebee_subscription_id

    where ia.total_amount_paid > 0

)

select * from subs_enriched
