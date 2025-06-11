{{ config(materialized='table') }}

SELECT 
  League,
  Match_Date,
  Matchweek,
  Team,
  Home_Away,
  Player,
  `Min`AS Minutes,
  
  Gls AS Goals,
  Ast AS Assists,
  Sh AS Shots,
  SoT AS Shots_On_Targets,
  xG_Expected AS xGoals,
  xAG_Expected AS xAGoals,
  
  Tkl AS Tackles,
  `Int` AS Interceptions,
  Blocks AS Blocks,
  
  Cmp_Passes AS Completed_Passes,
  PrgP_Passes AS Progressive_Passes,
  Touches AS Touches
  
FROM {{ ref('union_all') }}
