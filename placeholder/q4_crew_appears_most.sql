SELECT p.name, COUNT(c.person_id) AS cnt
FROM crew AS c JOIN people AS p
ON c.person_id = p.person_id
GROUP by c.person_id
ORDER BY cnt DESC
LIMIT 20;
