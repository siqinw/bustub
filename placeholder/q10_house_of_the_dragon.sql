WITH hotd_titles AS(
    SELECT titles.title_id as title_id, titles.primary_title as
    FROM titles 
    JOIN (
        SELECT title_id
        FROM titles
        WHERE primary_title = 'House of the Dragon'
    ) AS House
    ON hotd_titles.title_id = House.title_id
)

    SELECT primary_title
    FROM titles 
    JOIN (
        SELECT title_id
        FROM titles
        WHERE primary_title = 'House of the Dragon'
    ) AS House
    ON titles.title_id = House.title_id;
