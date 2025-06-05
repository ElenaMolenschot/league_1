{{ config(materialized='table') }}

WITH sub_pos AS (
    SELECT *, 
           SUBSTRING(Pos, 1, 2) AS Pos_1
    FROM {{ ref('union_all') }}
),

-- On repère la dernière équipe dans laquelle chaque joueur a joué
last_match_per_player AS (
    SELECT *
    FROM (
        SELECT *,
               ROW_NUMBER() OVER (PARTITION BY player ORDER BY Match_Date DESC) AS rn
        FROM sub_pos
    )
    WHERE rn = 1
),

-- On en extrait le nom du joueur et son équipe la plus récente
last_team_per_player AS (
    SELECT player, team AS last_team
    FROM last_match_per_player
),

-- On garde toutes les lignes correspondant à cette dernière équipe
final_filtered AS (
    SELECT s.*
    FROM sub_pos s
    INNER JOIN last_team_per_player l
        ON s.player = l.player AND s.team = l.last_team
)

SELECT *
FROM final_filtered
