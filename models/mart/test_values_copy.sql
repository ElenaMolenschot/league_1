SELECT * FROM {{ ref('test') }}
WHERE Market_value_eur IS NOT NULL AND Team LIKE "%Como%"