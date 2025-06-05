{{ config(materialized='table') }}
SELECT * FROM {{ source('div', 'eredivisie_ado_den_haag') }}
UNION ALL
SELECT * FROM {{ source('div', 'eredivisie_afcajax') }}
UNION ALL
SELECT * FROM {{ source('div', 'eredivisie_az-alkmaar') }}
UNION ALL
SELECT * FROM {{ source('div', 'eredivisie_fc-emmen') }}
UNION ALL
SELECT * FROM {{ source('div', 'eredivisie_fc-twente') }}
UNION ALL 
SELECT * FROM {{ source('div', 'eredivisie_fc-utrecht') }}
UNION ALL
SELECT * FROM {{ source('div', 'eredivisie_feyenoord-rotterdam') }}
UNION ALL 
SELECT * FROM {{ source('div', 'eredivisie_fortuna-sittard') }}
UNION ALL 
SELECT * FROM {{ source('div', 'eredivisie_heracles-almelo') }}
UNION ALL 
SELECT * FROM {{ source('div', 'eredivisie_pec-zwolle') }}
UNION ALL 
SELECT * FROM {{ source('div', 'eredivisie_psv-eindhoven') }}
UNION ALL 
SELECT * FROM {{ source('div', 'eredivisie_rkc-waalwijk') }}
UNION ALL 
SELECT * FROM {{ source('div', 'eredivisie_sc-heerenveen') }}
UNION ALL 
SELECT * FROM {{ source('div', 'eredivisie_vitesse') }}
UNION ALL 
SELECT * FROM {{ source('div', 'eredivisie_vvv-venlo') }}
UNION ALL 
SELECT * FROM {{ source('div', 'eredivisie_willem-ii') }}