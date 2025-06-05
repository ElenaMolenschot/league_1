WITH stats_raw AS (
  SELECT
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
  GROUP BY Player, Team, Pos_1
)

SELECT
  Player,
  Team,
  Nombre_Matchs,
  Pos_1,
    CASE
        -- Gardiens
        WHEN Pos_1 IN ('GK') THEN 'Gardien'

        -- Buteurs
        WHEN Pos_1 IN ('ST', 'CF', 'FW', 'LF', 'RF') THEN 'Buteur'

        -- Ailiers
        WHEN Pos_1 IN ('LW') THEN 'Ailier gauche'
        WHEN Pos_1 IN ('RW') THEN 'Ailier droit'

        -- Milieux offensifs
        WHEN Pos_1 IN ('CAM', 'AM', 'LAM', 'RAM') THEN 'Milieu offensif'

        -- Milieux latéraux
        WHEN Pos_1 IN ('LM') THEN 'Milieu gauche'
        WHEN Pos_1 IN ('RM') THEN 'Milieu droit'

        -- Milieux centraux / relayeurs
        WHEN Pos_1 IN ('CM', 'RCM', 'LCM') THEN 'Milieu relayeur'

        -- Milieux défensifs
        WHEN Pos_1 IN ('CDM', 'DM', 'LDM', 'RDM') THEN 'Milieu défensif'

        -- Défenseurs centraux
        WHEN Pos_1 IN ('CB', 'LCB', 'RCB') THEN 'Défenseur central'

        -- Latéraux
        WHEN Pos_1 IN ('LB', 'RB', 'LWB', 'RWB', 'WB') THEN 'Lateral'

        -- Par défaut
        ELSE 'Inconnu'
    END AS Poste_simplifie,
  `Min`,
  ROUND(total_Gls / NULLIF(`Min`, 0) * 90, 2) AS Gls_per90,
  ROUND(total_Ast / NULLIF(`Min`, 0) * 90, 2) AS Ast_per90,
  ROUND(total_PK / NULLIF(`Min`, 0) * 90, 2) AS PK_per90,
  ROUND(total_PKatt / NULLIF(`Min`, 0) * 90, 2) AS PKatt_per90,
  ROUND(total_Sh / NULLIF(`Min`, 0) * 90, 2) AS Sh_per90,
  ROUND(total_SoT / NULLIF(`Min`, 0) * 90, 2) AS SoT_per90,
  ROUND(total_CrdY / NULLIF(`Min`, 0) * 90, 2) AS CrdY_per90,
  ROUND(total_CrdR / NULLIF(`Min`, 0) * 90, 2) AS CrdR_per90,
  ROUND(total_Touches / NULLIF(`Min`, 0) * 90, 2) AS Touches_per90,
  ROUND(total_Tkl / NULLIF(`Min`, 0) * 90, 2) AS Tkl_per90,
  ROUND(total_Int / NULLIF(`Min`, 0) * 90, 2) AS Int_per90,
  ROUND(total_Blocks / NULLIF(`Min`, 0) * 90, 2) AS Blocks_per90,
  ROUND(total_xG_Expected / NULLIF(`Min`, 0) * 90, 2) AS xG_Expected_per90,
  ROUND(total_npxG_Expected / NULLIF(`Min`, 0) * 90, 2) AS npxG_Expected_per90,
  ROUND(total_xAG_Expected / NULLIF(`Min`, 0) * 90, 2) AS xAG_Expected_per90,
  ROUND(total_SCA_SCA / NULLIF(`Min`, 0) * 90, 2) AS SCA_SCA_per90,
  ROUND(total_GCA_SCA / NULLIF(`Min`, 0) * 90, 2) AS GCA_SCA_per90,
  ROUND(total_Cmp_Passes / NULLIF(`Min`, 0) * 90, 2) AS Cmp_Passes_per90,
  ROUND(total_Att_Passes / NULLIF(`Min`, 0) * 90, 2) AS Att_Passes_per90,
  ROUND(total_PrgP_Passes / NULLIF(`Min`, 0) * 90, 2) AS PrgP_Passes_per90,
  ROUND(total_Carries_Carries / NULLIF(`Min`, 0) * 90, 2) AS Carries_Carries_per90,
  ROUND(total_Att_Take_Ons / NULLIF(`Min`, 0) * 90, 2) AS Att_Take_Ons_per90,
  ROUND(total_Succ_Take_Ons / NULLIF(`Min`, 0) * 90, 2) AS Succ_Take_Ons_per90
FROM stats_raw
