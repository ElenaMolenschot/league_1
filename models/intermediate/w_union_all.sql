SELECT * FROM {{ ref('w_rename_ligue1') }}
UNION ALL
SELECT * FROM {{ ref('w_rename_premier_league') }}
UNION ALL
SELECT * FROM {{ ref('w_rename_seriea') }}
UNION ALL
SELECT * FROM {{ ref('w_rename_serieb') }}
UNION ALL
SELECT * FROM {{ source('div', 'w_bundesliga') }}
UNION ALL 
SELECT * FROM {{ ref('w_temp_cast_eredivisie') }}
UNION ALL
SELECT * FROM {{ source('div', 'w_liga') }}
UNION ALL 
SELECT * FROM {{ source('div', 'w_premeira_liga') }}
UNION ALL 
SELECT * FROM {{ source('div', 'w_belgian_pro_league') }}