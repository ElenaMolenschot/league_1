SELECT 
  Player, 
  Team,
  Age,
  SUM(Min) AS Minutes,
  SUM(Tkl) AS Tackles,
  SUM(Int) AS Interceptions,
  SUM(Blocks) AS Blocks,
  SUM(Cmp_Passes) AS Completed_Passes,
  SUM(PrgP_Passes) AS Progressive_Passes
FROM {{ ref('union_all') }}
WHERE Pos = 'DM'
GROUP BY Player, Team, Age
ORDER BY SUM(Tkl) DESC
