WITH sub_score_99 AS (SELECT 
  Player,
  Team,
  Nombre_Matchs,
  Poste_simplifie,
  score_brut_standardise,
  ROUND(
    SAFE_DIVIDE(score_brut_standardise, MAX(score_brut_standardise) OVER (PARTITION BY Poste_simplifie)) * 99,
    2
  ) AS score_99
FROM {{ ref('int_stats_per90_normalize') }} )

SELECT * FROM sub_score_99
ORDER BY score_99 DESC 
