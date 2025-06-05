{{ config(materialized='table') }}
SELECT *
FROM {{ ref('w_cast_all') }}
WHERE weekly_wages IS NOT NULL