WITH kidman_movies AS(
    SELECT (crew.title_id) AS title_id
    FROM crew
    WHERE crew.person_id 
    IN (SELECT people.person_id FROM people WHERE name = 'Nicole Kidman')
)

SELECT name 
FROM crew
INNER JOIN people ON crew.person_id = people.person_id
WHERE (category = 'actor' OR category = 'actress') AND crew.title_id in kidman_movies
ORDER BY name;

