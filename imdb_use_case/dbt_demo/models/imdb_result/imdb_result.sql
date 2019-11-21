{{ config(materialized='table') }}

WITH movies AS(
SELECT * FROM {{ ref('movies')}}),
actors AS (
SELECT * FROM {{ ref('actors')}}),
directors AS(
SELECT * FROM {{ ref('directors')}}),
directors_genres AS(
SELECT * FROM {{ ref('directors_genres')}}),
movies_directors AS(
SELECT * FROM {{ ref('movies_directors')}}),
movies_genres AS(
SELECT * FROM {{ ref('movies_genres')}}),
roles AS(
SELECT * FROM {{ ref('roles')}})
SELECT movies.movie_id, movies.movie_name, movies.movie_year, movies.movie_rank,
movies_genres.genre, roles.role, directors.director_id, actors.actor_id, actors.actor_first_name, actors.actor_last_name, actors.actor_gender
FROM movies
JOIN movies_directors ON movies.movie_id = movies_directors.movie_id
JOIN movies_genres ON movies.movie_id = movies_genres.movie_id
JOIN roles ON movies.movie_id = roles.movie_id
JOIN directors ON directors.director_id = movies_directors.director_id
JOIN actors ON roles.actor_id = actors.actor_id