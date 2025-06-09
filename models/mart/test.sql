SELECT COUNT(*) FROM {{ ref('foot_table_final') }}
WHERE Market_value_eur IS NOT NULL