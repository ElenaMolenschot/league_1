SELECT 
  fp.League,
  fp.Player,
  fp.Team,
  fp.Age,
  fp.Nombre_Matchs,
  fp.Poste_simplifie,
  COALESCE(gk.score_99, fp.score_99) AS score_99
FROM {{ ref('int_top_players') }} AS fp
LEFT JOIN {{ ref('gk_int_top_players') }} AS gk
  ON fp.Player = gk.Player
  AND fp.Team = gk.Team
ORDER BY score_99 DESC