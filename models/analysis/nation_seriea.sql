SELECT 
  Nation, 
  COUNT(Nation) AS nombre_joueurs
FROM `speedy-defender-456808-e8.League_1.stats_joueurs_serie_a`
GROUP BY Nation
ORDER BY nombre_joueurs DESC
