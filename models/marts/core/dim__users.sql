{{
  config(
    materialized='incremental',
    unique_key="user_id"
  )
}}

with users as (

    select * from {{ ref('stg_prod__users') }}

)

select * from users
