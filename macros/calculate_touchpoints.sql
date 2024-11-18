{% macro calculate_touchpoints(rfr_source_column) %}
    sum(case when {{ rfr_source_column }} in ('Google', 'Youtube', 'Gmail') then 1 else 0 end) as google_touchpoints,
    sum(case when {{ rfr_source_column }} in ('Facebook', 'Instagram') then 1 else 0 end) as meta_touchpoints
{% endmacro %}

