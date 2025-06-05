{{ config(materialized='table') }}
SELECT 
    player
    ,ROUND((annual_wages / 12) * 1.19,2) as monthly_wages_eur
FROM {{ ref('w_clean_all') }}