{{ config(materialized="table") }}

{{ cast_all_gk("div", "gk_bundesliga") }}
union all
{{ cast_all_gk("div", "gk_premier_league") }}
union all
{{ cast_all_gk("div", "gk_eredivisie") }}
union all
{{ cast_all_gk("div", "gk_liga") }}
union all
{{ cast_all_gk("div", "gk_ligue_1") }}
union all
{{ cast_all_gk("div", "gk_serie_a") }}
union all
{{ cast_all_gk("div", "gk_serie_b") }}
union all
{{ cast_all_gk("div", "gk_belgian_pro_league") }}
union all
{{ cast_all_gk("div", "gk_primeira") }}