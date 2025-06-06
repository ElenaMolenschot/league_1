WITH players_multiple_teams AS (
  SELECT 
    Player
  FROM {{ ref('union_all') }}
  GROUP BY Player
  HAVING COUNT(DISTINCT Team) > 1
)

SELECT 
  i.Player,
  i.Team
FROM {{ ref('union_all') }} i
JOIN players_multiple_teams p
ON i.Player = p.Player
WHERE i.Player = "Vitinha"
GROUP BY i.Player, i.Team
ORDER BY i.Player
