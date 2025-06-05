SELECT * FROM {{ ref('w_rename_ligue1') }}
UNION ALL
SELECT * FROM {{ ref('w_rename_premier_league') }}
UNION ALL
SELECT * FROM {{ ref('w_rename_seriea') }}
UNION ALL
SELECT * FROM {{ ref('w_rename_serieb') }}
UNION ALL
SELECT * FROM {{ ref('w_rename_bundesliga') }}
UNION ALL 
SELECT * FROM {{ ref('w_rename_eredivisie') }}
UNION ALL
SELECT * FROM {{ ref('w_rename_liga') }}
UNION ALL 
SELECT * FROM {{ ref('w_rename_premeira_liga') }}
UNION ALL 
SELECT * FROM {{ ref('w_rename_belgium') }}