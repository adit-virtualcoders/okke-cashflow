with source as (
      select * from {{ source('segment_web', 'pages') }}
),
renamed as (
    select
        {{ adapter.quote("id") }} as event_id,
        {{ adapter.quote("anonymous_id") }},
        {{ adapter.quote("user_id") }},
        
        {{ adapter.quote("url") }},
        {{ adapter.quote("path") }},
        {{ adapter.quote("search") }},
        {{ adapter.quote("title") }},
        
        {{ adapter.quote("referrer") }},
        {{ adapter.quote("context_user_agent") }},

        {{ adapter.quote("context_campaign_id") }},
        {{ adapter.quote("context_campaign_content") }},
        {{ adapter.quote("context_campaign_medium") }},
        {{ adapter.quote("context_campaign_name") }},
        {{ adapter.quote("context_campaign_source") }},
        {{ adapter.quote("context_campaign_term") }},
        
        {{ adapter.quote("received_at") }},
        {{ adapter.quote("timestamp") }} as event_at

    from source
)
select * from renamed
