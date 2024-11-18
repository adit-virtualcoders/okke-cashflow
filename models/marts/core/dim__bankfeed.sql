{{
    config(
        materialized='incremental',
        unique_key="bank_feed_id",
    )
}}

with bank_feeds as (

    select * from {{ ref('stg_web__bank_feed') }}

)

select * from bank_feeds
