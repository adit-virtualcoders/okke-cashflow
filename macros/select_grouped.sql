{% macro select_grouped(tbl, cnt_distinct, group_by, col_name, date_group, where_filter=None) -%}

    select 
            datetrunc({{ date_group }}, {{ group_by }}) as date,
            count(distinct {{ cnt_distinct }}) as {{ col_name }}

    from {{ tbl }}

    group by datetrunc({{ date_group }}, {{ group_by }})

{%- endmacro %}