{{ config(materialized='table') }}
SELECT 
    player
    ,ROUND((annual_wages / 12) * 1.19,0) as monthly_wages_eur
    ,annual_wages
    ,club
FROM {{ ref('w_cast_all') }}