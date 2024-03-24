SELECT *
FROM {{table}}
{%- if limit %}
LIMIT {{limit}}
{%- endif %}
;