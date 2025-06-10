{{ config(materialized='table') }}

WITH stats_raw AS (
  SELECT
    League_clean AS League,
    Player,
    Team,
    Pos_1,
    Count(Match_Date) AS Nombre_Matchs,
    SUM(Min) AS `Min`,
    SUM(Gls) AS total_Gls,
    SUM(Ast) AS total_Ast,
    SUM(PK) AS total_PK,
    SUM(PKatt) AS total_PKatt,
    SUM(Sh) AS total_Sh,
    SUM(SoT) AS total_SoT,
    SUM(CrdY) AS total_CrdY,
    SUM(CrdR) AS total_CrdR,
    SUM(Touches) AS total_Touches,
    SUM(Tkl) AS total_Tkl,
    SUM(Int) AS total_Int,
    SUM(Blocks) AS total_Blocks,
    SUM(xG_Expected) AS total_xG_Expected,
    SUM(npxG_Expected) AS total_npxG_Expected,
    SUM(xAG_Expected) AS total_xAG_Expected,
    SUM(SCA_SCA) AS total_SCA_SCA,
    SUM(GCA_SCA) AS total_GCA_SCA,
    SUM(Cmp_Passes) AS total_Cmp_Passes,
    SUM(Att_Passes) AS total_Att_Passes,
    SUM(PrgP_Passes) AS total_PrgP_Passes,
    SUM(Carries_Carries) AS total_Carries_Carries,
    SUM(Att_Take_Ons) AS total_Att_Take_Ons,
    SUM(Succ_Take_Ons) AS total_Succ_Take_Ons
  FROM {{ ref('int_player_position') }}
  GROUP BY League, Player, Team, Pos_1
),
stats_with_poste AS (
  SELECT
    League,
    Player,
    Team,
    CASE
        WHEN Pos_1 IN ('GK') THEN 'Gardien'
        WHEN Pos_1 IN ('ST', 'CF', 'FW', 'LF', 'RF') THEN 'Buteur'
        WHEN Pos_1 = 'LW' THEN 'Ailier gauche'
        WHEN Pos_1 = 'RW' THEN 'Ailier droit'
        WHEN Pos_1 IN ('CAM', 'AM', 'LAM', 'RAM') THEN 'Milieu offensif'
        WHEN Pos_1 = 'LM' THEN 'Milieu gauche'
        WHEN Pos_1 = 'RM' THEN 'Milieu droit'
        WHEN Pos_1 IN ('CM', 'RCM', 'LCM') THEN 'Milieu relayeur'
        WHEN Pos_1 IN ('CDM', 'DM', 'LDM', 'RDM') THEN 'Milieu defensif'
        WHEN Pos_1 IN ('CB', 'LCB', 'RCB') THEN 'Defenseur central'
        WHEN Pos_1 IN ('LB', 'RB', 'LWB', 'RWB', 'WB') THEN 'Lateral'
        ELSE 'Inconnu'
    END AS Poste_simplifie,
    Nombre_Matchs,
    `Min`,
    total_Gls,
    total_Ast,
    total_PK,
    total_PKatt,
    total_Sh,
    total_SoT,
    total_CrdY,
    total_CrdR,
    total_Touches,
    total_Tkl,
    total_Int,
    total_Blocks,
    total_xG_Expected,
    total_npxG_Expected,
    total_xAG_Expected,
    total_SCA_SCA,
    total_GCA_SCA,
    total_Cmp_Passes,
    total_Att_Passes,
    total_PrgP_Passes,
    total_Carries_Carries,
    total_Att_Take_Ons,
    total_Succ_Take_Ons
  FROM stats_raw
),

final_agg AS (
  SELECT
    League,
    Player,
    Team,
    Poste_simplifie,
    SUM(Nombre_Matchs) AS Nombre_Matchs,
    SUM(`Min`) AS `Min`,

    ROUND(SUM(total_Gls) / NULLIF(SUM(`Min`), 0) * 90, 2) AS Gls_per90,
    ROUND(SUM(total_Ast) / NULLIF(SUM(`Min`), 0) * 90, 2) AS Ast_per90,
    ROUND(SUM(total_PK) / NULLIF(SUM(`Min`), 0) * 90, 2) AS PK_per90,
    ROUND(SUM(total_PKatt) / NULLIF(SUM(`Min`), 0) * 90, 2) AS PKatt_per90,
    ROUND(SUM(total_Sh) / NULLIF(SUM(`Min`), 0) * 90, 2) AS Sh_per90,
    ROUND(SUM(total_SoT) / NULLIF(SUM(`Min`), 0) * 90, 2) AS SoT_per90,
    ROUND(SUM(total_CrdY) / NULLIF(SUM(`Min`), 0) * 90, 2) AS CrdY_per90,
    ROUND(SUM(total_CrdR) / NULLIF(SUM(`Min`), 0) * 90, 2) AS CrdR_per90,
    ROUND(SUM(total_Touches) / NULLIF(SUM(`Min`), 0) * 90, 2) AS Touches_per90,
    ROUND(SUM(total_Tkl) / NULLIF(SUM(`Min`), 0) * 90, 2) AS Tkl_per90,
    ROUND(SUM(total_Int) / NULLIF(SUM(`Min`), 0) * 90, 2) AS Int_per90,
    ROUND(SUM(total_Blocks) / NULLIF(SUM(`Min`), 0) * 90, 2) AS Blocks_per90,
    ROUND(SUM(total_xG_Expected) / NULLIF(SUM(`Min`), 0) * 90, 2) AS xG_Expected_per90,
    ROUND(SUM(total_npxG_Expected) / NULLIF(SUM(`Min`), 0) * 90, 2) AS npxG_Expected_per90,
    ROUND(SUM(total_xAG_Expected) / NULLIF(SUM(`Min`), 0) * 90, 2) AS xAG_Expected_per90,
    ROUND(SUM(total_SCA_SCA) / NULLIF(SUM(`Min`), 0) * 90, 2) AS SCA_SCA_per90,
    ROUND(SUM(total_GCA_SCA) / NULLIF(SUM(`Min`), 0) * 90, 2) AS GCA_SCA_per90,
    ROUND(SUM(total_Cmp_Passes) / NULLIF(SUM(`Min`), 0) * 90, 2) AS Cmp_Passes_per90,
    ROUND(SUM(total_Att_Passes) / NULLIF(SUM(`Min`), 0) * 90, 2) AS Att_Passes_per90,
    ROUND(SUM(total_PrgP_Passes) / NULLIF(SUM(`Min`), 0) * 90, 2) AS PrgP_Passes_per90,
    ROUND(SUM(total_Carries_Carries) / NULLIF(SUM(`Min`), 0) * 90, 2) AS Carries_Carries_per90,
    ROUND(SUM(total_Att_Take_Ons) / NULLIF(SUM(`Min`), 0) * 90, 2) AS Att_Take_Ons_per90,
    ROUND(SUM(total_Succ_Take_Ons) / NULLIF(SUM(`Min`), 0) * 90, 2) AS Succ_Take_Ons_per90
  FROM stats_with_poste
  GROUP BY League, Player, Team, Poste_simplifie
)

SELECT * FROM final_agg
