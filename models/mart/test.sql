SELECT COUNT(*) FROM {{ ref('test2') }}
WHERE market_value_eur IS NOT NULL 