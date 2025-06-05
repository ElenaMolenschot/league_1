SELECT * FROM {{ ref('int_top_players') }}
INNER JOIN {{ ref('all_salaries_mktvalue') }}
USING(Player)