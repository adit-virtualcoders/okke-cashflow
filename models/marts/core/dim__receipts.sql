{{
config(
  materialized='incremental',
  unique_key="receipt_id"
)
}}

with receipts as (

    select * from {{ ref('stg_web__receipt') }}

)

select * from receipts
