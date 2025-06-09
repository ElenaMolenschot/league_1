SELECT 
Player,
tp.Team,
Nombre_Matchs,
Poste_simplifie,
score_brut,
score_99,
Monthly_wages,
Annual_Wages,
Market_value_eur

FROM {{ ref('int_top_players') }} AS tp
INNER JOIN {{ ref('all_salaries_mktvalue') }} AS mkt
USING(Player)