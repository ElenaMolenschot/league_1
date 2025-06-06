{{ config(materialized='table') }}

SELECT 
  S.Player,
  S.Team,
  S.League,
  S.Match_Date,
  S.Matchweek,
  S.Team,
  S.Home_Away,
  S.Player,
  S.Gls AS Goals,
  S.Ast AS Assists, 
  S.Sh As Shots,
  S.SoT AS Shots_On_Targets,
  S.Int AS Interceptions,
  S.Blocks,
  xG_Expected as xGoals,
  xAG_Expected as xAGoals,
  W.Monthly_wages,
  W.Annual_Wages,
  W.Market_value_eur,
  W.Team as Team_salaries
FROM (
    SELECT *, 
           LOWER(
             TRANSLATE(
               Player,
               "ÁÀÂÄÃÅÉÈÊËÍÌÎÏÓÒÔÖÕÚÙÛÜÝÑÇáàâäãåéèêëíìîïóòôöõúùûüýñç",
               "AAAAAAEEEEIIIIOOOOOUUUUYNCaaaaaaeeeeiiiiooooouuuuync"
             )
           ) AS player_key
    FROM {{ ref('union_all') }}
) AS S
INNER JOIN (
    SELECT *, 
           LOWER(
             TRANSLATE(
               Player,
               "ÁÀÂÄÃÅÉÈÊËÍÌÎÏÓÒÔÖÕÚÙÛÜÝÑÇáàâäãåéèêëíìîïóòôöõúùûüýñç",
               "AAAAAAEEEEIIIIOOOOOUUUUYNCaaaaaaeeeeiiiiooooouuuuync"
             )
           ) AS player_key
    FROM {{ ref('all_salaries_mktvalue') }}
) AS W
ON S.player_key = W.player_key
