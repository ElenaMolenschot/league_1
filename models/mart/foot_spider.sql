{{ config(materialized='table') }}

SELECT 
  League,
  Team,
  Player,
  Poste_simplifie,

  Gls_per90_ratio AS Goals,
  Ast_per90_ratio AS Assistances,
  SoT_per90_ratio AS Shot_on_Target,
  xG_Expected_per90_ratio AS xG,
  xAG_Expected_per90_ratio AS xAg,

  Tkl_per90_ratio AS Tackles,
  Int_per90_ratio AS Interceptions,
  Blocks_per90_ratio AS Blocks,
  Cmp_Passes_per90_ratio AS Completed_Passes,
  PrgP_Passes_per90_ratio AS Progressive_Passes,
  Touches_per90_ratio AS Touches

FROM {{ ref('int_stats_per90_normalize') }}
