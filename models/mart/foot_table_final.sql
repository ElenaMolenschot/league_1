{{ config(materialized='table') }}
<<<<<<< HEAD
SELECT * FROM {{ ref('int_top_players') }} 
INNER JOIN {{ ref('all_salaries_mktvalue') }} 
USING(Player)
=======

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
  FROM {{ ref('int_top_players') }}
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
  S.Age,
  S.Nombre_Matchs,
  S.Poste_simplifie,
  S.score_99,
  W.Market_value_eur,
  W.Team AS Team_salaries
FROM int_top AS S
LEFT JOIN players_dedup AS W
ON S.sorted_player_key = W.sorted_player_key
ORDER BY S.score_99 DESC
>>>>>>> 0922e3ddee755d38df8654687557ed3e7d3aa395
