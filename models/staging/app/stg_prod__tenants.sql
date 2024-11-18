with source as (
      select * from {{ source('prod', 'tenant') }}
),
renamed as (
    select
        {{ adapter.quote("TenantId") }} as tenant_id,
        {{ adapter.quote("SubscriptionBusinessId") }} as subscription_business_id,
        
        convert(datetime, {{ adapter.quote("CreatedAt") }}, 1)  as created_at,
        {{ adapter.quote("ModifiedAt") }} as modified_at

    from source
)
select * from renamed
