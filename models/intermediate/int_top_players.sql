{{ config(materialized='table') }}

WITH sub_score_99 AS (
  SELECT
    League,
    Player,
    Team,
    Nombre_Matchs,
    Poste_simplifie,
    ROUND(score_brut_standardise, 2) AS score_brut,
    ROUND(
      SAFE_DIVIDE(score_brut_standardise, MAX(score_brut_standardise) OVER (PARTITION BY Poste_simplifie)) * 99,
      2
    ) AS score_99
  FROM {{ ref('int_stats_per90_normalize') }}
),

age_by_player AS (
  SELECT 
    Player, League_clean,
    MAX(Age) AS Age
  FROM {{ ref('int_player_position') }}
  GROUP BY Player, League_clean
)

SELECT 
  s.League,
  s.Player,
  a.Age,
  s.Team,
  s.Nombre_Matchs,
  s.Poste_simplifie,
  s.score_99
  
FROM sub_score_99 s
LEFT JOIN age_by_player a
  ON s.Player = a.Player AND s.League = a.League_clean
ORDER BY score_99 DESC
