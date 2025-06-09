{{ config(materialized='table') }}

WITH int_top AS (
  SELECT 
    Player,
    Team,
    League,
    Match_Date,
    Matchweek,
    Home_Away,
    Gls,
    Ast,
    Sh,
    SoT,
    Int,
    Blocks,
    xG_Expected,
    xAG_Expected,
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

players_cleaned AS (
  SELECT 
    Player,
    Team,
    Market_value_eur,
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
  FROM {{ ref('players_values_eu') }}
),

players_dedup AS (
  SELECT *
  FROM (
    SELECT *,
           ROW_NUMBER() OVER (PARTITION BY sorted_player_key ORDER BY Market_value_eur DESC) AS rn
    FROM players_cleaned
  )
  WHERE rn = 1
)

SELECT 
  S.Player,
  S.Team,
  S.League,
  S.Match_Date,
  S.Matchweek,
  S.Home_Away,
  S.Gls AS Goals,
  S.Ast AS Assists, 
  S.Sh AS Shots,
  S.SoT AS Shots_On_Targets,
  S.Int AS Interceptions,
  S.Blocks,
  S.xG_Expected AS xGoals,
  S.xAG_Expected AS xAGoals,
  W.Market_value_eur,
  W.Team AS Team_salaries
FROM int_top AS S
INNER JOIN players_dedup AS W
  ON S.sorted_player_key = W.sorted_player_key
