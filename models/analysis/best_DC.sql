SELECT 
  Player, 
  Team,
  Age,
  Pos,
  SUM(Min) AS Minutes,
  SUM(Tkl) AS Tackles,
  SUM(Int) AS Interceptions,
  SUM(Blocks) AS Blocks,
  SUM(Cmp_Passes) AS Completed_Passes,
  SUM(CrdY) AS Yellow_Cards,
  SUM(CrdR) AS Red_Cards
FROM {{ ref('union_all') }}
WHERE Pos = 'CB'
GROUP BY Player, Team, Age, Pos
ORDER BY Tackles DESC
