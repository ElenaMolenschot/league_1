SELECT 
  Player, 
  Team, 
  League,
  SUM(Gls) AS total_but
FROM {{ ref('union_all') }}
GROUP BY 
  Player,
  Team,
  League
HAVING 
  total_but > 0
ORDER BY 
  total_but DESC
