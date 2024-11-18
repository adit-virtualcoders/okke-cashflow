with source as (
      select * from {{ source('prod', 'subscription_user') }}
),
renamed as (
    select
        {{ adapter.quote("SubscriptionUserId") }} as subscription_user_id,
        {{ adapter.quote("SubscriptionId") }} as subscription_id,
        {{ adapter.quote("UserId") }} as user_id,
        {{ adapter.quote("SubscriptionUserAssociationId") }} association_id,

        {{ adapter.quote("Name") }} as name,

        {{ adapter.quote("CreatedAt") }} as created_at,
        {{ adapter.quote("ModifiedAt") }} as modified_at

    from source
)
select * from renamed
