{{ config(materialized='table') }}

SELECT 
    player_name, 
    market_value_eur, 
    team
FROM {{ ref('stg_div__valeurs_joueurs') }}

UNION ALL

SELECT 
    player_name, 
    market_value_eur, 
    team
FROM {{ ref('stg_div__uefa_youth_league_2024') }}

UNION ALL

SELECT 
    player_name, 
    market_value_eur, 
    team
FROM {{ ref('stg_div__laliga_valeurs') }}