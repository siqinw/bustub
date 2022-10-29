SELECT primary_title, premiered, runtime_minutes || ' (mins)'
FROM  (SELECT * FROM titles ORDER BY runtime_minutes DESC)
WHERE genres LIKE '%Sci-Fi%'
LIMIT 10;
