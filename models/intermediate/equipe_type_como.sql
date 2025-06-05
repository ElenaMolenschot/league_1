SELECT 
  Player, 
  Team, 
  SUM(Min) AS Minutes,
  SUM(Gls) AS Goals, 
  SUM(Ast) AS Assists
FROM `speedy-defender-456808-e8.League_1.stats_joueurs_serie_a`
WHERE Team = 'Como'
GROUP BY Player, Team
HAVING Minutes >= 10
ORDER BY Minutes, Goals DESC
