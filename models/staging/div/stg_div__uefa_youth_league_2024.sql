with source as (

    select * from {{ source('div', 'uefa_youth_league_2024') }}

),

renamed as (

    select
        string_field_0 AS player_name,
        string_field_2 AS team,

        -- Convert market value to numeric
        CASE
            -- Format "1,50 mio. €"
            WHEN string_field_1 LIKE '%mio.%' THEN
                CAST(REPLACE(SUBSTR(string_field_1, 1, STRPOS(string_field_1, 'mio.') - 2), ',', '.') AS FLOAT64) * 1000000

            -- Format "900 K €"
            WHEN string_field_1 LIKE '%K €%' THEN
                CAST(REPLACE(SUBSTR(string_field_1, 1, STRPOS(string_field_1, 'K') - 1), ',', '.') AS FLOAT64) * 1000

            -- Format "50 Th. €"
            WHEN string_field_1 LIKE '%Th.%' THEN
                CAST(REPLACE(SUBSTR(string_field_1, 1, STRPOS(string_field_1, 'Th.') - 2), ',', '.') AS FLOAT64) * 1000

            -- Format "€1.50m"
            WHEN string_field_1 LIKE '€%m' THEN
                CAST(REPLACE(REGEXP_EXTRACT(string_field_1, r'€([0-9]+(?:\.[0-9]+)?)m'), ',', '.') AS FLOAT64) * 1000000

            -- Format "€350k"
            WHEN string_field_1 LIKE '€%k' THEN
                CAST(REPLACE(REGEXP_EXTRACT(string_field_1, r'€([0-9]+(?:\.[0-9]+)?)k'), ',', '.') AS FLOAT64) * 1000

            ELSE NULL
        END AS market_value_eur

    from source

)

select * from renamed