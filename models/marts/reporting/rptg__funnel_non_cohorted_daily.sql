{% set date_group = 'day' %}
{% set date_interval = 'dd' %}

{{ select_noncohorted_metrics(date_group, date_interval) }}
