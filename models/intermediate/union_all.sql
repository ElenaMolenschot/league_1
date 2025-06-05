{{ config(materialized='table') }}

{{ cast_all('div', 'premier_league') }}
UNION ALL
({{ cast_all('div', 'liga') }})
UNION ALL
{{ cast_all('div', 'bundesliga') }}
UNION ALL
{{ cast_all('div', 'serie_a') }}
UNION ALL
{{ cast_all('div', 'serie_b') }}
UNION ALL
{{ cast_all('div', 'ligue_1') }}
UNION ALL
{{ cast_all('div', 'eredivisie') }}
UNION ALL
({{ cast_all('div', 'primeira_liga') }})
UNION ALL
({{ cast_all('div', 'belgian_pro_league') }})


