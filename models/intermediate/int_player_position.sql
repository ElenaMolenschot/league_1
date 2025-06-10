{{ config(materialized='table') }}

WITH sub_pos AS (
    SELECT *, 
           SUBSTRING(Pos, 1, 2) AS Pos_1
    FROM {{ ref('union_all') }}
),

-- Étape préliminaire : renommer certaines ligues
renamed_league AS (
    SELECT *,
        CASE 
            WHEN League = 'Belgian Pro League' THEN 'Belgian'
            WHEN League = 'Primeira Liga' THEN 'Portuguese'
            WHEN League = 'Eredivisie' THEN 'Netherlands'
            WHEN League = 'Fußball-Bundesliga' THEN 'Bundesliga'
            ELSE League
        END AS League_clean
    FROM sub_pos
),

-- Étape 1 : date max pour chaque joueur (dernier match joué)
last_match_per_player AS (
    SELECT player, MAX(Match_Date) AS last_match_date
    FROM renamed_league
    GROUP BY player
),

-- Étape 2 : on identifie le ou les clubs joués il y a moins de 60 jours avant cette date
teams_played_recently AS (
    SELECT DISTINCT s.player, s.team
    FROM renamed_league s
    JOIN last_match_per_player l
      ON s.player = l.player
     AND DATE_DIFF(l.last_match_date, s.Match_Date, DAY) BETWEEN 0 AND 60
),

-- Étape 3 : on récupère tous les matchs joués par ces joueurs avec ce club
final_filtered AS (
    SELECT s.*
    FROM renamed_league s
    JOIN teams_played_recently t
      ON s.player = t.player
     AND s.team = t.team
)

SELECT *
FROM final_filtered