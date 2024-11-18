{{
  config(
    materialized='table'
  )
}}

with 

users as (

    select * from {{ ref('dim__users') }}

),

tenants as (

    select * from {{ ref('dim__tenants') }}

), 

bank_feed as (

    select * from {{ ref('dim__bankfeed') }}

),

subscriptions as (

    select * from {{ ref('int__subscriptions_invoices') }}

),

invoices as (

    select *,
        row_number() over (partition by tenant_id order by email_sent_at asc) as invoice_rank

    from {{ ref('dim__invoices') }}

),

payments as (

    select *,
        row_number() over (partition by tenant_id order by created_at asc) as payments_rank
 
    from {{ ref('dim__payments')}}

),

receipts as (

    select *,
        row_number() over (partition by tenant_id order by created_at asc) as receipt_rank
 
    from {{ ref('dim__receipts')}}

),

bankfeed_agged as (

    select tenant_id, min(created_at) as first_bankfeed_at
 
    from bank_feed

    group by tenant_id

),

tenants_dates as (

    select
        tenants.tenant_id,
        tenants.chargebee_subscription_id,

        datetrunc(day, tenants.created_at) as date,
        tenants.created_at as tenant_created_at,
        invoices.email_sent_at as first_invoice_sent_at,
        payments.created_at as first_expense_created_at,

        least(
            invoices.created_at,
            payments.created_at,
            receipts.created_at,
            subscriptions.activated_at,
            bankfeed_agged.first_bankfeed_at
        ) as engaged_at

    from tenants

    left join invoices on tenants.tenant_id = invoices.tenant_id 

        and invoice_rank = 2

    left join payments on tenants.tenant_id = payments.tenant_id 

        and payments_rank = 1

    left join receipts on tenants.tenant_id = receipts.tenant_id 

        and receipt_rank = 1

    left join bankfeed_agged on tenants.tenant_id = bankfeed_agged.tenant_id

    left join subscriptions on tenants.chargebee_subscription_id = subscriptions.chargebee_subscription_id

)

select * from tenants_dates
