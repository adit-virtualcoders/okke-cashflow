with source as (
      select * from {{ source('meta_ads', 'insights') }}
),
renamed as (
    select
        {{ adapter.quote("id") }},
        {{ adapter.quote("received_at") }},
        {{ adapter.quote("social_spend") }},
        {{ adapter.quote("unique_clicks") }},
        {{ adapter.quote("impressions") }},
        {{ adapter.quote("clicks") }},
        {{ adapter.quote("frequency") }},
        {{ adapter.quote("reach") }},
        {{ adapter.quote("unique_impressions") }},
        {{ adapter.quote("uuid_ts") }},
        {{ adapter.quote("date_stop") }},
        {{ adapter.quote("date_start") }},
        {{ adapter.quote("inline_post_engagements") }},
        {{ adapter.quote("link_clicks") }},
        {{ adapter.quote("spend") }},
        {{ adapter.quote("ad_id") }}

    from source
)
select * from renamed
  