SELECT 
    player,
    SAFE_CAST(REPLACE(REPLACE(weekly_wages, '£', ''), ',', '') AS NUMERIC) AS weekly_wages,
    SAFE_CAST(REPLACE(REPLACE(annual_wages, '£', ''), ',', '') AS NUMERIC) AS annual_wages,
    club
FROM {{ ref('w_union_all') }}
    