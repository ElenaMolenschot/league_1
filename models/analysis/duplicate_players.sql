SELECT 
  *
FROM {{ ref('int_top_players') }}
WHERE Player = "Brice Samba"
