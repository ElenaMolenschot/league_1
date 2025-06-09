{{ config(materialized='table') }}

WITH sub_score_99 AS (SELECT 
  Player,
  Team,
  Nombre_Matchs,
  Pos_1,
  ROUND(score_brut_standardise,2) AS score_brut,
  ROUND(
    SAFE_DIVIDE(score_brut_standardise, MAX(score_brut_standardise) OVER (PARTITION BY Pos_1)) * 99,
    2
  ) AS score_99
FROM {{ ref('gk_int_stats_per90_normalize') }} )

SELECT * FROM sub_score_99
ORDER BY score_99 DESC 
