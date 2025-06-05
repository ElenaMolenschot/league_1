{{ config(materialized='table') }}
WITH join_sub AS (SELECT 
    W.player
    , M.player_name
    , ROUND(W.monthly_wages_eur,0) as monthly_wages_eur
    , W.annual_wages
    , ROUND(M.market_value_eur,0) as market_value_eur
    , W.club
FROM {{ ref('salaires_all') }} as W
FULL OUTER JOIN {{ ref('players_values_eu') }} as M
ON W.player = M.player_name)

, nonull_sub AS (SELECT *
FROM join_sub
WHERE NOT (player IS NULL AND market_value_eur IS NULL)
)

SELECT DISTINCT *
FROM nonull_sub