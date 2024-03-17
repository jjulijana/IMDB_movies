CREATE TABLE movies (
    id SERIAL PRIMARY KEY,
    movie_title TEXT,
    color TEXT,
    director_name TEXT,
    duration INT,
    gross INT,
    movie_imdb_link TEXT,
    language TEXT,
    country TEXT,
    content_rating TEXT,
    aspect_ratio FLOAT,
    imdb_score FLOAT,
    movie_facebook_likes INT
);