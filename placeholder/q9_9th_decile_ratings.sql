WITH actors_and_movies_1955 AS (
     SELECT
          people.person_id,
          people.name,
          titles.title_id,
          titles.primary_title
     FROM
          people
     INNER JOIN
          crew ON people.person_id = crew.person_id
     INNER JOIN
          titles ON crew.title_id = titles.title_id
     WHERE people.born = 1955 AND titles.type = "movie"
),
actor_ratings AS (
     SELECT
          name,
          ROUND(AVG(ratings.rating), 2) as rating
     FROM ratings
     INNER JOIN actors_and_movies_1955 ON ratings.title_id = actors_and_movies_1955.title_id
     GROUP BY actors_and_movies_1955.person_id
),
quartiles AS (
     SELECT *, NTILE(10) OVER (ORDER BY rating ASC) AS RatingQuartile FROM actor_ratings
)

SELECT name, rating
FROM quartiles
WHERE RatingQuartile = 9
ORDER BY rating DESC, name ASC;
