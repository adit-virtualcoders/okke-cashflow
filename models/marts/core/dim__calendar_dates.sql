{{ 
    config(
        materialized='incremental',
        unique_key='date'
    ) 
}}

{% set date_init = '2022-06-01' %}

select 

    dateadd(dd, value, '{{ date_init }}') as date,
    dateadd(dd, 1, eomonth(dateadd(dd, value, '{{ date_init }}'),-1)) month_start_date,
    eomonth(dateadd(dd, value, '{{ date_init }}'),0) month_end_date,
    format(dateadd(dd, value, '{{ date_init }}'),'yyyy-MM (MMM)') month_year,
    cast(
        year(dateadd(dd, value, '{{ date_init }}')) * 100 + month(dateadd(dd, value, '{{ date_init }}'))
        as varchar
    ) year_month,
    month(dateadd(dd, value, '{{ date_init }}')) month_number,
    dateadd(dd, 1, eomonth(dateadd(dd, value, '{{ date_init }}'),-1* (case when month(dateadd(dd, value, '{{ date_init }}'))%3=0 then  3 else month(dateadd(dd, value, '{{ date_init }}'))%3 end ) )) qtr_start_date,
    eomonth(dateadd(dd, value, '{{ date_init }}'), (case when month(dateadd(dd, value, '{{ date_init }}'))%3=0 then 0  else (3- (month(dateadd(dd, value, '{{ date_init }}'))%3)) end ) ) qtr_end_date,
    
    'Q' + cast(datepart(q, dateadd(dd, value, '{{ date_init }}')) as varchar) quarter,
    'Q' + cast(datepart(q, dateadd(dd, value, '{{ date_init }}')) as varchar) 
        + '-' 
        + cast(year(dateadd(dd, value, '{{ date_init }}')) as varchar) quarter_year,
    cast(
        year(dateadd(dd, value, '{{ date_init }}')) * 100 + datepart(q,dateadd(dd, value, '{{ date_init }}'))
        as varchar
    ) year_quarter,
    datepart(q,dateadd(dd, value, '{{ date_init }}')) qtr_no,

    dateadd(dd, 1, eomonth(dateadd(dd, value, '{{ date_init }}'),-1* (month(dateadd(dd, value, '{{ date_init }}')) ))) year_start_date,
    eomonth(dateadd(dd, value, '{{ date_init }}'),12-1* (month(dateadd(dd, value, '{{ date_init }}')) )) year_end_date,
    year(dateadd(dd, value, '{{ date_init }}')) year,

    datepart(dw,dateadd(dd, value, '{{ date_init }}')) weekday,
    format(dateadd(dd, value, '{{ date_init }}'),'ddd') weekday_name ,
    dateadd(dd, -1*datepart(dw,dateadd(dd, value, '{{ date_init }}'))+1,dateadd(dd, value, '{{ date_init }}')) week_start_date,
    dateadd(dd, -1*datepart(dw,dateadd(dd, value, '{{ date_init }}'))+7,dateadd(dd, value, '{{ date_init }}')) week_end_date,
    datepart(wk,dateadd(dd, value, '{{ date_init }}')) weeknum,
    cast(
        year(dateadd(dd, value, '{{ date_init }}'))*100+ datepart(wk,dateadd(dd, value, '{{ date_init }}'))
        as varchar
    ) year_week

from generate_series(0, datediff(dd, '{{ date_init }}', getdate()), 1)
