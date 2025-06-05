SELECT 
  Player, 
  Team,
  Pos,
  SUM(Min) AS Minutes,
  SUM(Cmp_Passes) AS Passes_Reussies,
  SUM(PrgP_Passes) AS Passes_Progressives,
  SUM(Ast) AS Assists,
  ROUND(SUM(xAG_Expected), 2) AS Expected_Assists,
  SUM(Carries_Carries) AS Conduites,
  SUM(PrgC_Carries) AS Conduites_Progressives,
  SUM(Touches) AS Touches,
  SUM(SAFE_CAST(Succ_Take_Ons AS FLOAT64)) AS Dribbles_Reussis
FROM `speedy-defender-456808-e8.League_1.stats_joueurs_serie_a`
WHERE Pos like "CM"
GROUP BY Player, Team, Pos
ORDER BY Passes_Reussies DESC