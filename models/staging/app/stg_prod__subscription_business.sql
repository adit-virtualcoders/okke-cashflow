with source as (
      select * from {{ source('prod', 'subscription_business') }}
),
renamed as (
    select
        {{ adapter.quote("SubscriptionId") }} as subscription_id,        
        {{ adapter.quote("BusinessName") }} as chargebee_subscription_id,

        {{ adapter.quote("SubscriptionBusinessId") }} as subscription_business_id,
        {{ adapter.quote("SubscriptionBusinessStatusId") }} as subscription_business_status_id,

        {{ adapter.quote("CreatedAt") }} as created_at,
        {{ adapter.quote("ModifiedAt") }} as modified_at

    from source
)
select * from renamed