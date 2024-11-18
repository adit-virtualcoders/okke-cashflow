{{
    config(
        materialized='incremental',
        unique_key="tenant_id",
    )
}}

with documents as (

    select * from {{ ref('stg_web__document') }}

)

select * from documents
