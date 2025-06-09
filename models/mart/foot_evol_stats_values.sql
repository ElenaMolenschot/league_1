{{ config(materialized='table') }}

WITH int_top AS (
  SELECT *, 
         LOWER(
           TRANSLATE(
             ARRAY_TO_STRING(
               ARRAY(
                 SELECT word 
                 FROM UNNEST(SPLIT(Player, ' ')) AS word 
                 ORDER BY word
               ), ' '
             ),
             "ÁÀÂÄÃÅÉÈÊËÍÌÎÏÓÒÔÖÕÚÙÛÜÝÑÇáàâäãåéèêëíìîïóòôöõúùûüýñç",
             "AAAAAAEEEEIIIIOOOOOUUUUYNCaaaaaaeeeeiiiiooooouuuuync"
           )
         ) AS sorted_player_key
  FROM {{ ref('union_all') }}
),

players_dedup AS (
  SELECT *
  FROM (
    SELECT *,
           LOWER(
             TRANSLATE(
               ARRAY_TO_STRING(
                 ARRAY(
                   SELECT word 
                   FROM UNNEST(SPLIT(Player, ' ')) AS word 
                   ORDER BY word
                 ), ' '
               ),
               "ÁÀÂÄÃÅÉÈÊËÍÌÎÏÓÒÔÖÕÚÙÛÜÝÑÇáàâäãåéèêëíìîïóòôöõúùûüýñç",
               "AAAAAAEEEEIIIIOOOOOUUUUYNCaaaaaaeeeeiiiiooooouuuuync"
             )
           ) AS sorted_player_key,
           ROW_NUMBER() OVER (PARTITION BY 
             LOWER(
               TRANSLATE(
                 ARRAY_TO_STRING(
                   ARRAY(
                     SELECT word 
                     FROM UNNEST(SPLIT(Player, ' ')) AS word 
                     ORDER BY word
                   ), ' '
                 ),
                 "ÁÀÂÄÃÅÉÈÊËÍÌÎÏÓÒÔÖÕÚÙÛÜÝÑÇáàâäãåéèêëíìîïóòôöõúùûüýñç",
                 "AAAAAAEEEEIIIIOOOOOUUUUYNCaaaaaaeeeeiiiiooooouuuuync"
               )
             )
           ORDER BY Market_value_eur DESC) AS rn
    FROM {{ ref('players_values_eu') }}
  )
  WHERE rn = 1
)


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
  W.Market_value_eur,
  W.Team as Team_salaries
FROM int_top AS S
INNER JOIN players_dedup AS W
ON S.sorted_player_key = W.sorted_player_key