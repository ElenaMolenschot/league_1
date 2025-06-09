{{ config(materialized='table') }}

WITH int_top AS (
  SELECT *,
         LOWER(
           TRANSLATE(Player,
             "ÁÀÂÄÃÅÉÈÊËÍÌÎÏÓÒÔÖÕÚÙÛÜÝÑÇáàâäãåéèêëíìîïóòôöõúùûüýñç",
             "AAAAAAEEEEIIIIOOOOOUUUUYNCaaaaaaeeeeiiiiooooouuuuync"
           )
         ) AS player_clean
  FROM {{ ref('int_top_players') }}
),

players_cleaned AS (
  SELECT *,
         LOWER(
           TRANSLATE(Player,
             "ÁÀÂÄÃÅÉÈÊËÍÌÎÏÓÒÔÖÕÚÙÛÜÝÑÇáàâäãåéèêëíìîïóòôöõúùûüýñç",
             "AAAAAAEEEEIIIIOOOOOUUUUYNCaaaaaaeeeeiiiiooooouuuuync"
           )
         ) AS player_clean
  FROM {{ ref('players_values_eu') }}
),

-- On génère toutes les paires joueur court vs joueur long
cross_match AS (
  SELECT
    S.Player,
    S.League,
    S.Team,
    S.Age,
    S.Nombre_Matchs,
    S.Poste_simplifie,
    S.score_99,
    W.Player AS Player_ref,
    W.Market_value_eur,
    W.Team AS Team_salaries,
    W.player_clean AS player_clean_ref,
    SPLIT(S.player_clean, ' ') AS player_words
  FROM int_top AS S
  CROSS JOIN players_cleaned AS W
),

-- On vérifie que TOUS les mots du joueur court apparaissent dans le nom du joueur long
filtered_matches AS (
  SELECT *,
    (
      SELECT COUNT(*) 
      FROM UNNEST(player_words) AS word
      WHERE player_clean_ref LIKE CONCAT('%', word, '%')
    ) AS match_count,
    ARRAY_LENGTH(player_words) AS total_words
  FROM cross_match
),

-- On garde les correspondances complètes
valid_matches AS (
  SELECT *,
         ROW_NUMBER() OVER (PARTITION BY Player ORDER BY Market_value_eur DESC) AS rn
  FROM filtered_matches
  WHERE match_count = total_words
),

players_dedup AS (
  SELECT *
  FROM valid_matches
  WHERE rn = 1
)

SELECT
  S.League,
  S.Player,
  S.Team,
  S.Age,
  S.Nombre_Matchs,
  S.Poste_simplifie,
  S.score_99,
  W.Market_value_eur,
  W.Team_salaries
FROM int_top AS S
LEFT JOIN players_dedup AS W
  ON S.Player = W.Player
ORDER BY S.score_99 DESC
