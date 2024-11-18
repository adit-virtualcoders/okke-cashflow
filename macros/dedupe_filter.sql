{% macro dedupe_filter(email) -%}

    not (
        {{ email }} like '%cashflow-manager%'

        or {{ email }} like '%cashflowmanager%'

        or {{ email }} like '%framecreate.com%'

        or {{ email }} like '%localhost%'

        or {{ email }} like '%ikaros.io%'
    )

{%- endmacro %}

