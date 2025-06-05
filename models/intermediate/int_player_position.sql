
WITH sub_pos AS (SELECT * 
, SUBSTRING(Pos, 1, 2) AS Pos_1
FROM {{ ref('union_all') }}
WHERE League = "Ligue 1")

SELECT
    *
FROM sub_pos

