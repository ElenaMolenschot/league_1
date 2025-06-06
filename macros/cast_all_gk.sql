#{% macro cast_all_gk(schema_name, table_name) %}
SELECT
  League,
  SAFE_CAST(Match_Date AS DATE) AS Match_Date,
  SAFE_CAST(REGEXP_REPLACE(SAFE_CAST(Matchweek AS STRING), r'[^0-9]', '') AS INT64) AS Matchweek,
  Home_Team,
  Home_Formation,
  SAFE_CAST(Home_Score AS INT64) AS Home_Score,
  SAFE_CAST(Home_xG AS FLOAT64) AS Home_xG,
  Home_Goals,
  SAFE_CAST(Home_Yellow_Cards AS INT64) AS Home_Yellow_Cards,
  SAFE_CAST(Home_Red_Cards AS INT64) AS Home_Red_Cards,
  Away_Team,
  Away_Formation,
  SAFE_CAST(Away_Score AS INT64) AS Away_Score,
  SAFE_CAST(Away_xG AS FLOAT64) AS Away_xG,
  Away_Goals,
  SAFE_CAST(Away_Yellow_Cards AS INT64) AS Away_Yellow_Cards,
  SAFE_CAST(Away_Red_Cards AS INT64) AS Away_Red_Cards,
  Game_URL,
  Team,
  Home_Away,
  Player,
  Nation,
  SAFE_CAST(REGEXP_EXTRACT(SAFE_CAST(Age AS STRING), r'^\d{2}') AS INT64) AS Age,
  SAFE_CAST(Min AS INT64) AS Min,
  SAFE_CAST(SoTA_Shot_Stopping AS INT64) AS SoTA_Shot_Stopping,
  SAFE_CAST(NULLIF(SAFE_CAST(GA_Shot_Stopping AS STRING), 'NA') AS INT64) AS GA_Shot_Stopping,
  SAFE_CAST(NULLIF(SAFE_CAST(Saves_Shot_Stopping AS STRING), 'NA') AS INT64) AS Saves_Shot_Stopping,
  SAFE_CAST(
    NULLIF(
      REGEXP_REPLACE(SAFE_CAST(Save_percent_Shot_Stopping AS STRING), r'[%]', ''),
      'NA'
    ) AS FLOAT64
  ) AS Save_percent_Shot_Stopping,
  SAFE_CAST(PSxG_Shot_Stopping AS FLOAT64) AS PSxG_Shot_Stopping,
  SAFE_CAST(Cmp_Launched AS INT64) AS Cmp_Launched,
  SAFE_CAST(Att_Launched AS INT64) AS Att_Launched,
  SAFE_CAST(
    NULLIF(
      REGEXP_REPLACE(SAFE_CAST(Cmp_percent_Launched AS STRING), r'[%]', ''),
      'NA'
    ) AS FLOAT64
  ) AS Cmp_percent_Launched,
  SAFE_CAST(Att_GK_Passes AS INT64) AS Att_GK_Passes,
  SAFE_CAST(Thr_Passes AS INT64) AS Thr_Passes,
  SAFE_CAST(
    NULLIF(
      REGEXP_REPLACE(SAFE_CAST(Launch_percent_Passes AS STRING), r'[%]', ''),
      'NA'
    ) AS FLOAT64
  ) AS Launch_percent_Passes,
  SAFE_CAST(AvgLen_Passes AS FLOAT64) AS AvgLen_Passes,
  SAFE_CAST(Att_Goal_Kicks AS INT64) AS Att_Goal_Kicks,
  SAFE_CAST(
    NULLIF(
      REGEXP_REPLACE(SAFE_CAST(Launch_percent_Goal_Kicks AS STRING), r'[%]', ''),
      'NA'
    ) AS FLOAT64
  ) AS Launch_percent_Goal_Kicks,
  SAFE_CAST(
    NULLIF(
      REGEXP_REPLACE(SAFE_CAST(AvgLen_Goal_Kicks AS STRING), r'[%]', ''),
      'NA'
    ) AS FLOAT64
  ) AS AvgLen_Goal_Kicks,
  SAFE_CAST(Opp_Crosses AS INT64) AS Opp_Crosses,
  SAFE_CAST(NULLIF(SAFE_CAST(Stp_Crosses AS STRING), 'NA') AS INT64) AS Stp_Crosses,
  SAFE_CAST(
    NULLIF(
      REGEXP_REPLACE(SAFE_CAST(Stp_percent_Crosses AS STRING), r'[%]', ''),
      'NA'
    ) AS FLOAT64
  ) AS Stp_percent_Crosses,
  SAFE_CAST(NULLIF(SAFE_CAST(Player_NumOPA_Sweeper AS STRING), 'NA') AS INT64) AS Player_NumOPA_Sweeper,
  SAFE_CAST(
    NULLIF(
      REGEXP_REPLACE(SAFE_CAST(AvgDist_Sweeper AS STRING), r'[%]', ''),
      'NA'
    ) AS FLOAT64
  ) AS AvgDist_Sweeper
FROM {{ source(schema_name, table_name) }}
{% endmacro %}