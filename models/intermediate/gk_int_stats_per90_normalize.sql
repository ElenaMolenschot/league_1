{{ config(materialized='table') }}

WITH calcul_ratio AS (
  SELECT *,
    MAX(GA_per90) OVER () AS max_GA_per90,
    MAX(Saves_per90) OVER () AS max_Saves_per90,
    MAX(PSxG_per90) OVER () AS max_PSxG_per90,
    MAX(Stp_Crosses_per90) OVER () AS max_Stp_Crosses_per90,
    MAX(Opp_Crosses_per90) OVER () AS max_Opp_Crosses_per90,
    MAX(OPA_per90) OVER () AS max_OPA_per90,
    MAX(Cmp_Launched_per90) OVER () AS max_Cmp_Launched_per90,
    MAX(Att_Launched_per90) OVER () AS max_Att_Launched_per90,
    MAX(Save_percent) OVER () AS max_Save_percent,
    MAX(Clean_Sheet_percent) OVER () AS max_Clean_Sheet_percent
  FROM {{ ref('gk_int_stats_per90') }}
  WHERE Nombre_Matchs > 5 AND `Min` >= 300
),

gk_ranking_global_score AS (
  SELECT *,
    ROUND(SAFE_DIVIDE(GA_per90, max_GA_per90), 2) AS GA_per90_ratio,
    ROUND(1 - SAFE_DIVIDE(GA_per90, max_GA_per90), 2) AS Inv_GA_per90_ratio,
    ROUND(SAFE_DIVIDE(Saves_per90, max_Saves_per90), 2) AS Saves_per90_ratio,
    ROUND(SAFE_DIVIDE(PSxG_per90, max_PSxG_per90), 2) AS PSxG_per90_ratio,
    ROUND(SAFE_DIVIDE(Stp_Crosses_per90, max_Stp_Crosses_per90), 2) AS Stp_Crosses_per90_ratio,
    ROUND(SAFE_DIVIDE(Opp_Crosses_per90, max_Opp_Crosses_per90), 2) AS Opp_Crosses_per90_ratio,
    ROUND(SAFE_DIVIDE(OPA_per90, max_OPA_per90), 2) AS OPA_per90_ratio,
    ROUND(SAFE_DIVIDE(Cmp_Launched_per90, max_Cmp_Launched_per90), 2) AS Cmp_Launched_per90_ratio,
    ROUND(SAFE_DIVIDE(Att_Launched_per90, max_Att_Launched_per90), 2) AS Att_Launched_per90_ratio,
    ROUND(SAFE_DIVIDE(Save_percent, max_Save_percent), 2) AS Save_Percent_ratio,
    ROUND(SAFE_DIVIDE(Clean_Sheet_percent, max_Clean_Sheet_percent), 2) AS Clean_Sheet_percent_ratio
  FROM calcul_ratio
)

SELECT
  Player,
  Team,
  Nombre_Matchs,
  'GK' AS Pos_1,
  GA_per90,
  Saves_per90,
  PSxG_per90,
  Save_percent,
  Clean_Sheet_percent,
  Stp_Crosses_per90,
  Opp_Crosses_per90,
  OPA_per90,
  Cmp_Launched_per90,
  Att_Launched_per90,

  ROUND(
    (
      0.25 * Save_Percent_ratio +            -- Efficacité d’arrêt
      0.20 * Clean_Sheet_percent_ratio +     -- % de clean sheets
      0.15 * Inv_GA_per90_ratio +            -- Peu de buts encaissés
      0.10 * Saves_per90_ratio +             -- Volume d’arrêts
      0.10 * PSxG_per90_ratio +              -- Qualité des arrêts
      0.10 * OPA_per90_ratio +               -- Sorties hautes et jeu libre
      0.05 * Cmp_Launched_per90_ratio +      -- Précision longues relances
      0.05 * Att_Launched_per90_ratio        -- Volume longues relances
    ) * LEAST(Nombre_Matchs / 20.0, 1.0),    -- Pénalité si peu de matchs
    4
  ) AS score_brut_standardise

FROM gk_ranking_global_score
