{{ config(materialized='table') }}
SELECT 
    player
    , weekly_wages as weekly_wages_gbp
    , annual_wages as annual_wages_gbp
    , age 
    , pos
FROM {{ ref('w_cast_all') }}
WHERE weekly_wages IS NOT NULL