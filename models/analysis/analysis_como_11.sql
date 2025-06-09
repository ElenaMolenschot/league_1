SELECT Player, Team, Poste_simplifie, score_99, Nombre_Matchs FROM {{ ref('int_top_players') }}
WHERE Team = "Como" AND player IN (
    'Nicolás Paz',
    'Ignace Van Der Brempt',
    'Maxence Caqueret',
    'Yannik Engelhardt',
    'Álex Valle',
    'Máximo Perrone',
    'Alberto Dossena',
    'Assane Diao',
    'Anastasios Douvikas',
    'Jonathan Ikone',
    'Jean Butez'
)





