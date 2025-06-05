SELECT 
    `Nom du joueur` as player
    ,SAFE_CAST(`Salaire hebdomadaire` AS STRING) AS weekly_wages
    , SAFE_CAST(`Salaire annuel` AS STRING) AS annual_wages
    , `âge` as age
    , `Poste` as pos
    , `Nationalité` as nation
    , `Club` as club
FROM {{ source('div', 'w_eredivisie') }}
WHERE `âge` NOT LIKE "%NA%"