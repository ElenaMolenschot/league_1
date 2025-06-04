SELECT 
    string_field_0 AS player
    , string_field_1 AS weekly_wages
    , string_field_2 AS annual_wages
    , string_field_3 AS age
    , string_field_4 AS pos
    , string_field_5 AS nation 
FROM {{ source('div', 'w_serie_b') }}
WHERE string_field_3 NOT LIKE "%NA%"