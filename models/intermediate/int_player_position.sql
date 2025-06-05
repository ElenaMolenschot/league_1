{{ config(materialized='table') }}

WITH sub_pos AS (SELECT * 
, SUBSTRING(Pos, 1, 2) AS Pos_1
FROM {{ ref('union_all') }})

SELECT *
   
FROM sub_pos


