SELECT 
    string_field_1 AS player
    , string_field_2 AS weekly_wages
    , string_field_3 AS yearly_wages
    , string_field_4 AS age
    , string_field_5 AS pos
    , string_field_6 AS nation 
FROM {{ source('div', 'w_premier_league') }}
WHERE string_field_4 NOT LIKE "%NA%"