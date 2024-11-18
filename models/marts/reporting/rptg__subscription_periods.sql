with

{% set date_init = '2023-01-01' %}

subscriptions as (

    select * from {{ ref('int__subscriptions_invoices') }}

),

invoices as (

    select * from {{ ref('dim__chargebee_invoices') }}

),

date_grouped as (
        
    select 
        datetrunc(month, 
            dateadd(mm, value, '{{ date_init }}')
        ) as date
    
    from generate_series(0, datediff(mm, '{{ date_init }}', getdate()), 1)

),

subscription_periods as (

    select
        dg.date,
        s.chargebee_subscription_id,
        s.subscription_status,
        s.activated_at,
        s.cancelled_at,
        sum(amount_paid) amount_paid

    from date_grouped as dg

    left join subscriptions as s 

    on (dg.date >= datetrunc(month, s.activated_at))

        and (
            dg.date < datetrunc(month, s.cancelled_at)
            or s.cancelled_at is null
        )

    left join invoices i

        on s.chargebee_subscription_id = i.chargebee_subscription_id
        
        and dg.date = datetrunc(month, i.paid_at)

    group by dg.date, s.chargebee_subscription_id, s.subscription_status, s.activated_at, s.cancelled_at

),

init_subs as (
    
    select
        datetrunc(month, activated_at) as activated_month,
        count(distinct chargebee_subscription_id) as initial_subscriptions

    from subscription_periods

    group by datetrunc(month, activated_at)

),

enriched as (

    select 
        sp.*,
        datetrunc(month, activated_at) as activated_month,
        datediff(mm, activated_at, sp.date) + 1 as [period],

        s.initial_subscriptions

    from subscription_periods sp

    left join init_subs s on datetrunc(month, sp.activated_at) = s.activated_month

)

select * from enriched
