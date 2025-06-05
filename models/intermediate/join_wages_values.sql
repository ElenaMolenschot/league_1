SELECT 
    W.player
    , M.player_name
    , ROUND(W.monthly_wages_eur,0) as monthly_wages_eur
    , ROUND(M.market_value_eur,0) as market_value_eur
FROM {{ ref('final_wages_all') }} as W
FULL OUTER JOIN {{ ref('players_values_eu') }} as M
ON W.player = M.player_name