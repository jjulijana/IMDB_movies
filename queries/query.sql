SELECT *
FROM movie_genres
{%- if limit %}
LIMIT {{limit}}
{%- endif %}
;