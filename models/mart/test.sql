SELECT * FROM {{ ref('foot_table_final') }}
WHERE market_value_eur IS NULL