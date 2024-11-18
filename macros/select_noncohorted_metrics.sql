{% macro select_noncohorted_metrics(date_group, date_interval) -%}

    {% set date_init = '2023-01-01' %}

    with 

    pages as (

        select * from {{ ref('fct__pages') }}

    ),

    users as (

        select * from {{ ref('dim__users') }}

    ),

    subscriptions as (

        select * from {{ ref('int__subscriptions_invoices') }}

    ),

    tenants as (

        select * from {{ ref('dim__tenants') }}

    ),

    app_invoices as (

        select * from {{ ref('dim__invoices') }}

    ),

    app_expenses as (

        select * from {{ ref('dim__payments') }}

    ),

    engagements as (

        select * from {{ ref('int__engagements') }}

    ),

    google_ads as (

        select *,
            row_number() over (partition by date_start, base_campaign_id order by received_at desc) as rn

        from {{ ref('stg_google_ads__campaign_performance') }}

    ),

    meta_ads as (

        select *,
            row_number() over (partition by date_start, ad_id order by received_at desc) as rn
        
        from {{ ref('stg_meta_ads__insights') }}

    ),

    date_grouped as (
        
        select 
            datetrunc({{ date_group }}, 
                dateadd({{ date_interval }}, value, '{{ date_init }}')
            ) as date
        
        from generate_series(0, datediff({{ date_interval }}, '{{ date_init }}', getdate()), 1)

    ),

    pages_grouped as (

        select 
            datetrunc({{ date_group }}, event_at) as date,
            count(distinct anonymous_id) as visitors,
            count(distinct event_id) as page_views

        from pages

        group by datetrunc({{ date_group }}, event_at)

    ),

    google_ads_grouped as (

        select
            datetrunc({{ date_group }}, cast(date_start as date)) as date,
            sum(clicks) as google_clicks,
            sum(impressions) as google_impressions,
            sum(cost)/1E6 as google_spend

        from google_ads

        where rn = 1

        group by datetrunc({{ date_group }}, cast(date_start as date))

    ),

    meta_ads_grouped as (

        select
            datetrunc({{ date_group }}, cast(date_start as date)) as date,
            sum(clicks) as meta_clicks,
            sum(impressions) as meta_impressions,
            sum(spend) as meta_spend

        from meta_ads

        where rn = 1

        group by datetrunc({{ date_group }}, cast(date_start as date))

    ),

    -- tbl, cnt_distinct, group_by, col_name
    {% set group_metrics = [
        ('users', 'user_id', 'created_at', 'user_signups'),
        ('subscriptions', 'chargebee_subscription_id', 'trial_start', 'trials_started'),

        ('tenants', 'tenant_id', 'created_at', 'tenants_created'),
        ('engagements', 'tenant_id', 'engaged_at', 'tenants_engaged'),

        ('subscriptions', 'chargebee_subscription_id', 'activated_at', 'subscriptions_started'),
        ('subscriptions', 'chargebee_subscription_id', 'cancelled_at', 'subscriptions_cancelled'),
        
        ('app_invoices', 'customer_invoice_id', 'email_sent_at', 'invoices_sent'),
        ('app_expenses', 'payment_id', 'created_at', 'expenses_logged'),

    ] %}


    {%- for mtrc in group_metrics %}

        {{ mtrc.3 }}_vw as (

            {{ select_grouped(mtrc.0, mtrc.1, mtrc.2, mtrc.3, date_group) }}

        ),

    {%- endfor %}

    current_active as (

        select 
            count(chargebee_subscription_id) as active_subs

        from subscriptions
        where 
            activated_at < current_timestamp        

            and (
                current_timestamp < cancelled_at
                or cancelled_at is null
                )
    ),

    joined as (

        select 
        
            date_grouped.date,
            pages_grouped.page_views,
            pages_grouped.visitors,
            
            {%- for mtrc in group_metrics %}

            {{ mtrc.3 }}_vw.{{ mtrc.3 }},

            {%- endfor %}

            meta_ads_grouped.meta_clicks,
            meta_ads_grouped.meta_impressions,
            meta_ads_grouped.meta_spend,
            
            google_ads_grouped.google_clicks,
            google_ads_grouped.google_impressions,
            google_ads_grouped.google_spend,

            current_active.active_subs

        from date_grouped

        left join pages_grouped on date_grouped.date = pages_grouped.date

        {%- for mtrc in group_metrics %}

            left join {{ mtrc.3 }}_vw on date_grouped.date = {{ mtrc.3 }}_vw.date

        {%- endfor %}

        left join google_ads_grouped on date_grouped.date = google_ads_grouped.date
        
        left join meta_ads_grouped on date_grouped.date = meta_ads_grouped.date

        left join current_active on 1 = 1

    )

    select * from joined

{%- endmacro %}