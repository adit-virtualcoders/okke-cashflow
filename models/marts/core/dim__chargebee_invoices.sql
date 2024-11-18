{{
    config(
        materialized='incremental',
        unique_key="chargebee_invoice_id",
    )
}}

with 

invoices as (

    select * from {{ ref('stg_chargebee__invoices') }}

)

select * from invoices
