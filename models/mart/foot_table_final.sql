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
           ROW_NUMBER() OVER (
             PARTITION BY LOWER(
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
             ORDER BY Market_value_eur DESC
           ) AS rn
    FROM {{ ref('players_values_eu') }}
  )
  WHERE rn = 1
),

-- CROSS JOIN + filtrage souple
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
  LEFT JOIN players_dedup AS W ON TRUE  -- on croise tout
)

-- on garde les matchs avec tous les mots inclus
SELECT * EXCEPT (common_words, total_words)
FROM joined_players
WHERE common_words = total_words
ORDER BY score_99 DESC