{% macro compute_score_brut_standardise() %}
  0.20 * xG_Expected_per90_ratio +
  0.15 * SoT_per90_ratio +
  0.10 * Sh_per90_ratio +
  0.15 * xAG_Expected_per90_ratio +
  0.10 * SCA_SCA_per90_ratio +
  0.10 * PK_per90_ratio -
  0.05 * CrdY_per90_ratio -
  0.05 * CrdR_per90_ratio
{% endmacro %}

