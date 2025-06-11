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
)


  SELECT
  Player,
  Team,
  Pos_1,
  Nombre_Matchs,
  Min,
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