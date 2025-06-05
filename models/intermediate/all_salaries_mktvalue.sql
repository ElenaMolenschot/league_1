{{ config(materialized='table') }}

WITH join_sub AS (
    SELECT 
        COALESCE(W.player, M.player_name) AS Player,
        ROUND(W.monthly_wages_eur, 0) AS Monthly_wages,
        W.annual_wages AS Annual_Wages,
        ROUND(M.market_value_eur, 0) AS Market_value_eur,
        W.club AS Team
    FROM {{ ref('salaires_all') }} AS W
    FULL OUTER JOIN {{ ref('players_values_eu') }} AS M
    ON W.player = M.player_name
),

nonull_sub AS (
    SELECT *
    FROM join_sub
    WHERE NOT (player IS NULL AND market_value_eur IS NULL)
)

SELECT DISTINCT *
FROM nonull_sub
