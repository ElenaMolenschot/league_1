SELECT 
  S.Player, S.Team, S.Nombre_Matchs, S.Poste_simplifie, S.score_99,
  W.Monthly_wages, W.Annual_Wages, W.Market_value_eur, W.Team
FROM (
    SELECT *, 
           LOWER(
             TRANSLATE(
               Player,
               "ÁÀÂÄÃÅÉÈÊËÍÌÎÏÓÒÔÖÕÚÙÛÜÝÑÇáàâäãåéèêëíìîïóòôöõúùûüýñç",
               "AAAAAAEEEEIIIIOOOOOUUUUYNCaaaaaaeeeeiiiiooooouuuuync"
             )
           ) AS player_key
    FROM {{ ref('int_top_players') }}
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
WHERE S.Team = "Como"
ORDER BY score_99 DESC
