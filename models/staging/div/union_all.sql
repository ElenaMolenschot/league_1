{{ cast_all_as_string('div', 'premier_league') }}
UNION ALL
SELECT * EXCEPT(PrgC_Carries) FROM ({{ cast_all_as_string('div', 'liga') }})
UNION ALL
{{ cast_all_as_string('div', 'bundesliga') }}
UNION ALL
{{ cast_all_as_string('div', 'serie_a') }}
UNION ALL
{{ cast_all_as_string('div', 'serie_b') }}
UNION ALL
{{ cast_all_as_string('div', 'ligue_1') }}
UNION ALL
{{ cast_all_as_string('div', 'eredivisie') }}
UNION ALL
SELECT * EXCEPT(PrgC_Carries) FROM ({{ cast_all_as_string('div', 'primeira_liga') }})