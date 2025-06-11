{{ config(materialized='table') }}

WITH gk_prepared_data AS (
  SELECT
    Player,
    Team,
    SAFE_CAST(Min AS INT64) AS Min,
    SAFE_CAST(SoTA_Shot_Stopping AS INT64) AS SoTA,
    SAFE_CAST(GA_Shot_Stopping AS INT64) AS GA,
    SAFE_CAST(Saves_Shot_Stopping AS INT64) AS Saves,
    SAFE_CAST(Save_percent_Shot_Stopping AS FLOAT64) AS Save_percent,
    SAFE_CAST(PSxG_Shot_Stopping AS FLOAT64) AS PSxG,
    SAFE_CAST(Cmp_Launched AS INT64) AS Cmp_Launched,
    SAFE_CAST(Att_Launched AS INT64) AS Att_Launched,
    SAFE_CAST(Cmp_percent_Launched AS FLOAT64) AS Cmp_percent_Launched,
    SAFE_CAST(Att_GK_Passes AS INT64) AS Att_GK_Passes,
    SAFE_CAST(Thr_Passes AS INT64) AS Thr_Passes,
    SAFE_CAST(Launch_percent_Passes AS FLOAT64) AS Launch_percent_Passes,
    SAFE_CAST(AvgLen_Passes AS FLOAT64) AS AvgLen_Passes,
    SAFE_CAST(Att_Goal_Kicks AS INT64) AS Att_Goal_Kicks,
    SAFE_CAST(Launch_percent_Goal_Kicks AS FLOAT64) AS Launch_percent_Goal_Kicks,
    SAFE_CAST(AvgLen_Goal_Kicks AS FLOAT64) AS AvgLen_Goal_Kicks,
    SAFE_CAST(Opp_Crosses AS INT64) AS Opp_Crosses,
    SAFE_CAST(Stp_Crosses AS INT64) AS Stp_Crosses,
    SAFE_CAST(Stp_percent_Crosses AS FLOAT64) AS Stp_percent_Crosses,
    SAFE_CAST(Player_NumOPA_Sweeper AS INT64) AS Num_OPA,
    SAFE_CAST(AvgDist_Sweeper AS FLOAT64) AS Avg_OPA_Dist,
            CASE
      WHEN Home_Away = 'Home' AND SAFE_CAST(Away_Score AS INT64) = 0 THEN 1
      WHEN Home_Away = 'Away' AND SAFE_CAST(Home_Score AS INT64) = 0 THEN 1
      ELSE 0
    END AS is_clean_sheet
  FROM {{ ref('gk_union_all') }}
),

stats_raw_gk AS (
  SELECT
    Player,
    Team,
    'GK' AS Pos_1,
    COUNT(*) AS Nombre_Matchs,
    SUM(Min) AS Min,
    SUM(SoTA) AS total_SoTA,
    SUM(GA) AS total_GA,
    SUM(Saves) AS total_Saves,
    AVG(Save_percent) AS avg_Save_percent,
    SUM(PSxG) AS total_PSxG,
    SUM(Cmp_Launched) AS total_Cmp_Launched,
    SUM(Att_Launched) AS total_Att_Launched,
    AVG(Cmp_percent_Launched) AS avg_Cmp_percent_Launched,
    SUM(Att_GK_Passes) AS total_Att_GK_Passes,
    SUM(Thr_Passes) AS total_Thr_Passes,
    AVG(Launch_percent_Passes) AS avg_Launch_percent_Passes,
    AVG(AvgLen_Passes) AS avg_Len_Passes,
    SUM(Att_Goal_Kicks) AS total_Att_Goal_Kicks,
    AVG(Launch_percent_Goal_Kicks) AS avg_Launch_percent_Goal_Kicks,
    AVG(AvgLen_Goal_Kicks) AS avg_Len_Goal_Kicks,
    SUM(Opp_Crosses) AS total_Opp_Crosses,
    SUM(Stp_Crosses) AS total_Stp_Crosses,
    AVG(Stp_percent_Crosses) AS avg_Stp_percent_Crosses,
    SUM(Num_OPA) AS total_OPA,
    AVG(Avg_OPA_Dist) AS avg_OPA_Dist,
    SUM(is_clean_sheet) AS total_clean_sheets
  FROM gk_prepared_data
  GROUP BY Player, Team
),

gk_per90_stats AS (
  SELECT
    Player,
    Team,
    'GK' AS Pos_1,
    Nombre_Matchs,
    Min,
    ROUND(total_SoTA / NULLIF(Min, 0) * 90, 2) AS SoTA_per90,
    ROUND(total_GA / NULLIF(Min, 0) * 90, 2) AS GA_per90,
    ROUND(total_Saves / NULLIF(Min, 0) * 90, 2) AS Saves_per90,
    ROUND(avg_Save_percent, 2) AS Save_percent,
    ROUND(total_PSxG / NULLIF(Min, 0) * 90, 2) AS PSxG_per90,
    ROUND(total_Cmp_Launched / NULLIF(Min, 0) * 90, 2) AS Cmp_Launched_per90,
    ROUND(total_Att_Launched / NULLIF(Min, 0) * 90, 2) AS Att_Launched_per90,
    ROUND(avg_Cmp_percent_Launched, 2) AS Cmp_percent_Launched,
    ROUND(total_Att_GK_Passes / NULLIF(Min, 0) * 90, 2) AS Att_GK_Passes_per90,
    ROUND(total_Thr_Passes / NULLIF(Min, 0) * 90, 2) AS Thr_Passes_per90,
    ROUND(avg_Launch_percent_Passes, 2) AS Launch_percent_Passes,
    ROUND(avg_Len_Passes, 2) AS AvgLen_Passes,
    ROUND(total_Att_Goal_Kicks / NULLIF(Min, 0) * 90, 2) AS Att_Goal_Kicks_per90,
    ROUND(avg_Launch_percent_Goal_Kicks, 2) AS Launch_percent_Goal_Kicks,
    ROUND(avg_Len_Goal_Kicks, 2) AS AvgLen_Goal_Kicks,
    ROUND(total_Opp_Crosses / NULLIF(Min, 0) * 90, 2) AS Opp_Crosses_per90,
    ROUND(total_Stp_Crosses / NULLIF(Min, 0) * 90, 2) AS Stp_Crosses_per90,
    ROUND(avg_Stp_percent_Crosses, 2) AS Stp_percent_Crosses,
    ROUND(total_OPA / NULLIF(Min, 0) * 90, 2) AS OPA_per90,
    ROUND(avg_OPA_Dist, 2) AS Avg_OPA_Dist,
    ROUND(SAFE_DIVIDE(total_clean_sheets, Nombre_Matchs), 2) AS Clean_Sheet_percent
  FROM stats_raw_gk
)

SELECT * FROM gk_per90_stats
