    <<<<<<< HEAD
SELECT 
Player,
tp.Team,
Nombre_Matchs,
Poste_simplifie,
score_brut,
score_99,
Monthly_wages,
Annual_Wages,
Market_value_eur

FROM {{ ref('int_top_players') }} AS tp
INNER JOIN {{ ref('all_salaries_mktvalue') }} AS mkt
USING(Player)
=======
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
>>>>>>> b619317d6e3866aedde68274c509fad49f3522b0
