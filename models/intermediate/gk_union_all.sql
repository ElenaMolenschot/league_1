{{ config(materialized='table') }}

{{ cast_all_gk('div', 'gk_bundesliga') }}
UNION ALL
{{ cast_all_gk('div', 'gk_premier_league') }}
UNION ALL
{{ cast_all_gk('div', 'gk_eredivisie') }}
UNION ALL
{{ cast_all_gk('div', 'gk_liga') }}
UNION ALL
{{ cast_all_gk('div', 'gk_ligue_1') }}
UNION ALL
{{ cast_all_gk('div', 'gk_serie_a') }}
UNION ALL
{{ cast_all_gk('div', 'gk_serie_b') }}