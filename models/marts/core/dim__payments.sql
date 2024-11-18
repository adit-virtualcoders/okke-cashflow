{{
  config(
    materialized='incremental',
    unique_key="payment_id"
  )
}}

with payments as (

    select * from {{ ref('stg_web__payment') }}

)

select * from payments
