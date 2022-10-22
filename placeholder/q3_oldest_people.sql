SELECT name, died - born AS age
FROM people
WHERE  born>=1900
ORDER BY age DESC, name ASC
LIMIT 20;
