with source as (
      select * from {{ source('web', 'document') }}
),
renamed as (
    select
        {{ adapter.quote("TenantId") }} as tenant_id,
        {{ adapter.quote("DocumentId") }} as document_id,
        
        {{ adapter.quote("CreatedAt") }} as created_at,
        {{ adapter.quote("ModifiedAt") }} as modified_at

    from source
)
select * from renamed
  