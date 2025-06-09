{{ config(materialized='table') }}

WITH sub_pos AS (
    SELECT *, 
           SUBSTRING(Pos, 1, 2) AS Pos_1
    FROM {{ ref('union_all') }}
),

-- Étape 1 : date max pour chaque joueur (dernier match joué)
last_match_per_player AS (
    SELECT player, MAX(Match_Date) AS last_match_date
    FROM sub_pos
    GROUP BY player
),

-- Étape 2 : on identifie le ou les clubs joués il y a moins de 60 jours avant cette date
teams_played_recently AS (
    SELECT DISTINCT s.player, s.team
    FROM sub_pos s
    JOIN last_match_per_player l
      ON s.player = l.player
     AND DATE_DIFF(l.last_match_date, s.Match_Date, DAY) BETWEEN 0 AND 60
),

-- Étape 3 : on récupère tous les matchs joués par ces joueurs avec ce club
final_filtered AS (
    SELECT s.*
    FROM sub_pos s
    JOIN teams_played_recently t
      ON s.player = t.player
     AND s.team = t.team
)

SELECT *
FROM final_filtered