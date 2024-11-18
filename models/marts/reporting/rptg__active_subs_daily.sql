{% set date_init = '2023-01-01' %}
{% set date_group = 'day' %}
{% set date_interval = 'dd' %}

with

date_grouped as (
        
    select 
        datetrunc({{ date_group }}, 
            dateadd({{ date_interval }}, value, '{{ date_init }}')
        ) as date
    
    from generate_series(0, datediff({{ date_interval }}, '{{ date_init }}', getdate()), 1)

),

subscriptions as (

    select * from {{ ref('int__subscriptions_invoices') }}

),

active_subs as (


    select 
        d.date,
        s.chargebee_subscription_id

    from date_grouped d

    left join subscriptions s

    on s.activated_at < d.date
        
        and (
            d.date < s.cancelled_at
            or s.cancelled_at is null)

)

select * from active_subs
