with source as (
    select * from {{ source('web', 'notification_log') }}
),

renamed as (
    select
        {{ adapter.quote('CustomerId') }} as customer_id,
        {{ adapter.quote('TenantId') }} as tenant_id,
        {{ adapter.quote('CreatedAt') }} as created_at,
        {{ adapter.quote('ModifiedAt') }} as modified_at,
        {{ adapter.quote('LogEntry') }} as log_entry
    from source
)

select * from renamed
