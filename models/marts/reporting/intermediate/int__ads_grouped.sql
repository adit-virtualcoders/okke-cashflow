{{
  config(
    materialized='incremental',
    unique_key="date"
  )
}}

{% set date_group = 'day' %}
{% set date_interval = 'dd' %}

with
    
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

unioned as (

    select 
        google.*,
        meta.meta_clicks,
        meta.meta_impressions,
        meta.meta_spend

    from google_ads_grouped google

    left join meta_ads_grouped meta

    on google.date = meta.date

)

select * from unioned
