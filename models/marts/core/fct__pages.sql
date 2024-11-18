with pages as (

    select * from {{ ref('stg_segment_web__pages') }}

)

select * from pages
