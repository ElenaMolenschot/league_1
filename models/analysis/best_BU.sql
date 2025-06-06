SELECT 
  Player, 
  Team,
  Age,
  SUM(Min) AS Minutes,
  SUM(Gls) AS Goals,
  ROUND(SUM(xG_Expected), 2) AS xG,
  SUM(SoT) AS Shots_on_Target,
  SUM(Succ_Take_Ons) AS Successful_Dribbles,
  SUM(SCA_SCA) AS Shot_Creating_Actions,
  SUM(Ast) AS Assists
FROM {{ ref('union_all') }}
WHERE Pos = 'FW'
  AND Age BETWEEN 18 AND 26
GROUP BY Player, Team, Age
ORDER BY Goals DESC
