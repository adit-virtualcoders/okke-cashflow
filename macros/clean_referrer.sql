{% macro clean_referrer(referrer) -%}

    trim('/' from 
        replace(
            replace(
                replace({{ referrer }}, 'https://', ''),
                'http://', ''),
            'https://www.', ''
        )
    )


{%- endmacro %}