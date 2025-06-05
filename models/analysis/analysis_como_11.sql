SELECT * FROM {{ ref('int_top_players') }}
WHERE Team = "Como"