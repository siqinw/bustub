SELECT COUNT(DISTINCT title_id)
FROM titles
WHERE premiered = 
(SELECT premiered FROM titles WHERE primary_title = 'Army of Thieves')
;
