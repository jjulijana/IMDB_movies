CREATE TABLE movie_genres AS
SELECT DISTINCT
    mg.id AS id,
    m.id AS movie_id,
    m.movie_title AS title,
    mg.genres AS genres
FROM
    movies m
JOIN
    genres_tmp mg ON m.movie_title = mg.movie_title
ORDER BY
    mg.id;