{% macro attribution_cols(session_at, conversion_at, user_id, conv_event) -%}
    
    {{ attribution_first_touch(session_at, conversion_at, user_id, conv_event) }},

    {{ attribution_last_touch(session_at, conversion_at, user_id, conv_event) }},

    {{ attribution_linear(session_at, conversion_at, user_id, conv_event) }},

    {{ attribution_u_shaped(session_at, conversion_at, user_id, conv_event) }}

    -- Removed Time Decay Attribution

{%- endmacro %}
