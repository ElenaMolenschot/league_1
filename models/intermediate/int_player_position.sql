
WITH sub_pos AS (SELECT * 
, SUBSTRING(Pos, 1, 2) AS Pos_1
FROM {{ ref('union_all') }}
WHERE League = "Ligue 1")

SELECT
    *,
    CASE
        -- Gardiens
        WHEN Pos_1 IN ('GK') THEN 'Gardien'

        -- Buteurs
        WHEN Pos_1 IN ('ST', 'CF', 'FW', 'LF', 'RF') THEN 'Buteur'

        -- Ailiers
        WHEN Pos_1 IN ('LW') THEN 'Ailier gauche'
        WHEN Pos_1 IN ('RW') THEN 'Ailier droit'

        -- Milieux offensifs
        WHEN Pos_1 IN ('CAM', 'AM', 'LAM', 'RAM') THEN 'Milieu offensif'

        -- Milieux latéraux
        WHEN Pos_1 IN ('LM') THEN 'Milieu gauche'
        WHEN Pos_1 IN ('RM') THEN 'Milieu droit'

        -- Milieux centraux / relayeurs
        WHEN Pos_1 IN ('CM', 'RCM', 'LCM') THEN 'Milieu relayeur'

        -- Milieux défensifs
        WHEN Pos_1 IN ('CDM', 'DM', 'LDM', 'RDM') THEN 'Milieu défensif'

        -- Défenseurs centraux
        WHEN Pos_1 IN ('CB', 'LCB', 'RCB') THEN 'Défenseur central'

        -- Latéraux
        WHEN Pos_1 IN ('LB', 'RB', 'LWB', 'RWB', 'WB') THEN 'Latéral'

        -- Par défaut
        ELSE 'Inconnu'
    END AS Poste_simplifie
FROM sub_pos

