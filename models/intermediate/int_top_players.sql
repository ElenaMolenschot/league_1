SELECT 
  Player,
  Team,
  Nombre_Matchs,
  Poste_simplifie,
  score_brut_standardise,
  ROUND(
    SAFE_DIVIDE(score_brut_standardise, MAX(score_brut_standardise) OVER (PARTITION BY Poste_simplifie)) * 99,
    2
  ) AS score_brut_sur_99
FROM {{ ref('int_stats_per90_normalize') }}
WHERE Poste_simplifie = "Lateral"
ORDER BY score_brut_standardise DESC
