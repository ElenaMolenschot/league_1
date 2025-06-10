{{ config(materialized='table') }}

WITH sub_age AS (SELECT Player, Team, Poste_simplifie, Age, score_99, Nombre_Matchs FROM {{ ref('all_score_99') }}
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
))

SELECT AVG(Age)
FROM sub_age



