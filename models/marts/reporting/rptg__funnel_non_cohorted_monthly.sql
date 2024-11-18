{% set date_group = 'month' %}
{% set date_interval = 'mm' %}

{{ select_noncohorted_metrics(date_group, date_interval) }}
