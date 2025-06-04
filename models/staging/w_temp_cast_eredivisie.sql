SELECT 
    player,
    SAFE_CAST(weekly_wages AS STRING) AS weekly_wages,
    SAFE_CAST(annual_wages AS STRING) AS annual_wages,
    age,
    pos,
    nation
FROM {{ source('div', 'w_eredivisie') }}
    