{{ config(materialized='table') }}

WITH join_sub AS (
    SELECT 
        COALESCE(W.Player, M.Player) AS Player,
        ROUND(W.monthly_wages_eur, 0) AS Monthly_wages,
        W.annual_wages AS Annual_Wages,
        ROUND(M.market_value_eur, 0) AS Market_value_eur,
        W.club AS Club
        M.team AS Team
    FROM {{ ref('salaires_all') }} AS W
    FULL OUTER JOIN {{ ref('players_values_eu') }} AS M
    ON W.player = M.Player
),

-- Cas doublons de joueurs avec des salaires différents : on prend le salaire le plus haut
nonull_sub AS (
    SELECT *
    FROM join_sub
    WHERE NOT (player IS NULL AND market_value_eur IS NULL)
) ,ranked_players AS (
  SELECT *,
         ROW_NUMBER() OVER (PARTITION BY Player ORDER BY Annual_Wages DESC) AS rank
  FROM nonull_sub
)

SELECT * EXCEPT (`rank`)
FROM ranked_players
WHERE rank = 1
ORDER BY Player