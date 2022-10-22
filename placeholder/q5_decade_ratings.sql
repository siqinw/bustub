SELECT 
CAST(premiered/10*10 AS TEXT) || 's' AS decade,
ROUND(AVG(rating),2) AS avg, 
MAX(rating), 
MIN(rating), 
count(*) AS cnt

FROM 
    titles
INNER JOIN 
    ratings ON ratings.title_id = titles.title_id
WHERE premiered IS NOT NULL
GROUP BY decade
ORDER BY avg DESC, decade ASC;
