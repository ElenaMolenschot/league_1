SELECT 
  Player, 
  Team, 
  Pos, 
  Age, 
  SUM(Gls) AS Goals, 
  SUM(Ast) AS Assists, 
  ROUND(SUM(xG_Expected), 2) AS xG, 
  SUM(SoT) AS Shots_on_Target
FROM `speedy-defender-456808-e8.League_1.union_all`
WHERE Pos IN ('FW', 'LW', 'RW', 'AM')
  AND Age BETWEEN 16 AND 26
GROUP BY Player, Team, Pos, Age
ORDER BY Goals DESC
