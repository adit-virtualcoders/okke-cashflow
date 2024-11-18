{% set date_group = 'isoww' %}
{% set date_interval = 'ww' %}

{{ select_noncohorted_metrics(date_group, date_interval) }}
