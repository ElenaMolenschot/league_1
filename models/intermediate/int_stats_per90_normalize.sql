{{ config(materialized='table') }}

WITH 
calcul_ratio AS (
SELECT *,
MAX(Gls_per90) OVER () AS max_Gls_per90,
MAX(Ast_per90) OVER () AS max_Ast_per90,
MAX(PK_per90) OVER () AS max_PK_per90,
MAX(PKatt_per90) OVER () AS max_PKatt_per90,
MAX(Sh_per90) OVER () AS max_Sh_per90,
MAX(SoT_per90) OVER () AS max_SoT_per90,
MAX(CrdY_per90) OVER () AS max_CrdY_per90,
MAX(CrdR_per90) OVER () AS max_CrdR_per90,
MAX(Touches_per90) OVER () AS max_Touches_per90,
MAX(Tkl_per90) OVER () AS max_Tkl_per90,
MAX(Int_per90) OVER () AS max_Int_per90,
MAX(Blocks_per90) OVER () AS max_Blocks_per90,
MAX(xG_Expected_per90) OVER () AS max_xG_Expected_per90,
MAX(npxG_Expected_per90) OVER () AS max_npxG_Expected_per90,
MAX(xAG_Expected_per90) OVER () AS max_xAG_Expected_per90,
MAX(SCA_SCA_per90) OVER () AS max_SCA_SCA_per90,
MAX(GCA_SCA_per90) OVER () AS max_GCA_SCA_per90,
MAX(Cmp_Passes_per90) OVER () AS max_Cmp_Passes_per90,
MAX(Att_Passes_per90) OVER () AS max_Att_Passes_per90,
MAX(PrgP_Passes_per90) OVER () AS max_PrgP_Passes_per90,
MAX(Carries_Carries_per90) OVER () AS max_Carries_Carries_per90,
MAX(Att_Take_Ons_per90) OVER () AS max_Att_Take_Ons_per90,
MAX(Succ_Take_Ons_per90) OVER () AS max_Succ_Take_Ons_per90

FROM {{ ref('int_stats_per90') }}
WHERE Nombre_Matchs > 5 AND `Min` >= 300),

ranking_global_score AS (
SELECT *,
ROUND(SAFE_DIVIDE(Gls_per90, max_Gls_per90), 2) AS Gls_per90_ratio,
ROUND(SAFE_DIVIDE(Ast_per90, max_Ast_per90), 2) AS Ast_per90_ratio,
ROUND(SAFE_DIVIDE(PK_per90, max_PK_per90), 2) AS PK_per90_ratio,
ROUND(SAFE_DIVIDE(PKatt_per90, max_PKatt_per90), 2) AS PKatt_per90_ratio,
ROUND(SAFE_DIVIDE(Sh_per90, max_Sh_per90), 2) AS Sh_per90_ratio,
ROUND(SAFE_DIVIDE(SoT_per90, max_SoT_per90), 2) AS SoT_per90_ratio,
ROUND(SAFE_DIVIDE(CrdY_per90, max_CrdY_per90), 2) AS CrdY_per90_ratio,
ROUND(SAFE_DIVIDE(CrdR_per90, max_CrdR_per90), 2) AS CrdR_per90_ratio,
ROUND(SAFE_DIVIDE(Touches_per90, max_Touches_per90), 2) AS Touches_per90_ratio,
ROUND(SAFE_DIVIDE(Tkl_per90, max_Tkl_per90), 2) AS Tkl_per90_ratio,
ROUND(SAFE_DIVIDE(Int_per90, max_Int_per90), 2) AS Int_per90_ratio,
ROUND(SAFE_DIVIDE(Blocks_per90, max_Blocks_per90), 2) AS Blocks_per90_ratio,
ROUND(SAFE_DIVIDE(xG_Expected_per90, max_xG_Expected_per90), 2) AS xG_Expected_per90_ratio,
ROUND(SAFE_DIVIDE(npxG_Expected_per90, max_npxG_Expected_per90), 2) AS npxG_Expected_per90_ratio,
ROUND(SAFE_DIVIDE(xAG_Expected_per90, max_xAG_Expected_per90), 2) AS xAG_Expected_per90_ratio,
ROUND(SAFE_DIVIDE(SCA_SCA_per90, max_SCA_SCA_per90), 2) AS SCA_SCA_per90_ratio,
ROUND(SAFE_DIVIDE(GCA_SCA_per90, max_GCA_SCA_per90), 2) AS GCA_SCA_per90_ratio,
ROUND(SAFE_DIVIDE(Cmp_Passes_per90, max_Cmp_Passes_per90), 2) AS Cmp_Passes_per90_ratio,
ROUND(SAFE_DIVIDE(Att_Passes_per90, max_Att_Passes_per90), 2) AS Att_Passes_per90_ratio,
ROUND(SAFE_DIVIDE(PrgP_Passes_per90, max_PrgP_Passes_per90), 2) AS PrgP_Passes_per90_ratio,
ROUND(SAFE_DIVIDE(Carries_Carries_per90, max_Carries_Carries_per90), 2) AS Carries_Carries_per90_ratio,
ROUND(SAFE_DIVIDE(Att_Take_Ons_per90, max_Att_Take_Ons_per90), 2) AS Att_Take_Ons_per90_ratio,
ROUND(SAFE_DIVIDE(Succ_Take_Ons_per90, max_Succ_Take_Ons_per90), 2) AS Succ_Take_Ons_per90_ratio
FROM calcul_ratio
)

SELECT *,
  {{ compute_score_brut_standardise('Poste_simplifie') }} AS score_brut_standardise
 FROM ranking_global_score
