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
                 FROM UNNEST(SPLIT(TRIM(Player), ' ')) AS word
                 ORDER BY word
               ), ' '
             ),
             "ÁÀÂÄÃÅÉÈÊËÍÌÎÏÓÒÔÖÕÚÙÛÜÝÑÇáàâäãåéèêëíìîïóòôöõúùûüýñç",
             "AAAAAAEEEEIIIIOOOOOUUUUYNCaaaaaaeeeeiiiiooooouuuuync"
           )
         ) AS sorted_player_key
  FROM {{ ref('all_score_99') }}
),

players_dedup AS (
  SELECT *,
         LOWER(
           TRANSLATE(
             ARRAY_TO_STRING(
               ARRAY(
                 SELECT word
                 FROM UNNEST(SPLIT(TRIM(Player), ' ')) AS word
                 ORDER BY word
               ), ' '
             ),
             "ÁÀÂÄÃÅÉÈÊËÍÌÎÏÓÒÔÖÕÚÙÛÜÝÑÇáàâäãåéèêëíìîïóòôöõúùûüýñç",
             "AAAAAAEEEEIIIIOOOOOUUUUYNCaaaaaaeeeeiiiiooooouuuuync"
           )
         ) AS sorted_player_key,
         ROW_NUMBER() OVER (
           PARTITION BY LOWER(
             TRANSLATE(
               ARRAY_TO_STRING(
                 ARRAY(
                   SELECT word
                   FROM UNNEST(SPLIT(TRIM(Player), ' ')) AS word
                   ORDER BY word
                 ), ' '
               ),
               "ÁÀÂÄÃÅÉÈÊËÍÌÎÏÓÒÔÖÕÚÙÛÜÝÑÇáàâäãåéèêëíìîïóòôöõúùûüýñç",
               "AAAAAAEEEEIIIIOOOOOUUUUYNCaaaaaaeeeeiiiiooooouuuuync"
             )
           )
           ORDER BY Market_value_eur DESC
         ) AS rn
  FROM {{ ref('players_values_eu') }}
),

players_dedup_filtered AS (
  SELECT *
  FROM players_dedup
  WHERE rn = 1
),

joined_players AS (
  SELECT 
    S.League,
    S.Player,
    S.Team,
    S.Age,
    S.Nombre_Matchs,
    S.Poste_simplifie,
    S.score_99,
    W.Market_value_eur,
    W.Team AS Team_salaries,
    ARRAY_LENGTH(
      ARRAY(
        SELECT word
        FROM UNNEST(SPLIT(S.sorted_player_key, ' ')) AS word
        WHERE word IN UNNEST(SPLIT(W.sorted_player_key, ' '))
      )
    ) AS common_words,
    ARRAY_LENGTH(SPLIT(S.sorted_player_key, ' ')) AS total_words
  FROM int_top AS S
  LEFT JOIN players_dedup_filtered AS W ON TRUE
)
<<<<<<< HEAD
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
=======

SELECT * EXCEPT (common_words, total_words)
FROM joined_players
WHERE common_words = total_words

ORDER BY score_99 DESC
>>>>>>> 2a4efcaf591da9ffed23718b90d8af1fb3d24204
