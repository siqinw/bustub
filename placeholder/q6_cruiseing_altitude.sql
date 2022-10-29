WITH cruise62 AS (
    SELECT crew.title_id AS title_id
    FROM crew
    INNER JOIN people ON people.person_id = crew.person_id
    WHERE people.name LIKE '%Cruise%' AND people.born = 1962
)

SELECT primary_title, votes
FROM cruise62
INNER JOIN ratings ON cruise62.title_id = ratings.title_id
INNER JOIN titles ON titles.title_id = cruise62.title_id
ORDER BY votes DESC
LIMIT 10;
