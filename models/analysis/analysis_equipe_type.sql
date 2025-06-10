{{ config(materialized='table') }}

WITH sub_11 AS (SELECT Player, Team, Poste_simplifie, score_99, Nombre_Matchs 
FROM {{ ref('all_score_99') }}
WHERE Player IN (
    'Alessandro Bastoni',
    'Amir Rrahmani',
    'Gleison Bremer',
    'Éderson',
    'Marcus Thuram',
    'Mateo Retegui',
    'Mile Svilar',
    'Moise Kean',
    'Riccardo Orsolini',
    'Scott McTominay',
    'Tijjani Reijnders'
)
ORDER BY score_99 DESC)

SELECT AVG(score_99)
FROM sub_11
