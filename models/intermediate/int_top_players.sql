{{ config(materialized='table') }}

WITH sub_score_99 AS (
  SELECT 
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
    Player, 
    MAX(Age) AS Age
  FROM {{ ref('union_all') }}
  GROUP BY Player
)

SELECT 
  s.*,
  a.Age
FROM sub_score_99 s
LEFT JOIN age_by_player a
  ON s.Player = a.Player
ORDER BY score_99 DESC
