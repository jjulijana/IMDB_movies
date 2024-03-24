INSERT INTO {{table}} ({{columns|join(', ')}})
VALUES (
    {%- for column in columns %}
    %({{ column }})s{% if not loop.last %},
    {%- endif %}
    {%- endfor %}
)