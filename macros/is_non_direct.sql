{% macro is_non_direct(channel) -%}

    {{ channel }} not in (
        'direct / none',
        'internal'
    )

{%- endmacro %}