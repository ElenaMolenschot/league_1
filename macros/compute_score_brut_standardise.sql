{% macro compute_score_brut_standardise(Poste_simplifie) %}
  CASE
    WHEN {{ Poste_simplifie }} = 'Buteur' THEN
      0.20 * xG_Expected_per90_ratio +
      0.15 * SoT_per90_ratio +
      0.10 * Sh_per90_ratio +
      0.15 * xAG_Expected_per90_ratio +
      0.10 * SCA_SCA_per90_ratio +
      0.10 * PK_per90_ratio -
      0.05 * CrdY_per90_ratio -
      0.05 * CrdR_per90_ratio
     WHEN {{ Poste_simplifie }} = 'Milieu droit' OR {{ Poste_simplifie }} = 'Milieu gauche'
        OR {{ Poste_simplifie }} = 'Ailier droit' OR {{ Poste_simplifie }} = 'Ailier gauche' THEN
      0.15 * Succ_Take_Ons_per90_ratio +
      0.15 * xAG_Expected_per90_ratio +
      0.15 * xG_Expected_per90_ratio +
      0.15 * SoT_per90_ratio +
      0.15 * PrgP_Passes_per90_ratio +
      0.10 * SCA_SCA_per90_ratio -
      0.05 * CrdY_per90_ratio -
      0.05 * CrdR_per90_ratio
    WHEN {{ Poste_simplifie }} = 'Milieu offensif' THEN
      0.20 * Ast_per90_ratio +
      0.15 * xAG_Expected_per90_ratio +
      0.15 * SCA_SCA_per90_ratio +
      0.10 * GCA_SCA_per90_ratio +
      0.10 * PrgP_Passes_per90_ratio +
      0.10 * Succ_Take_Ons_per90_ratio +
      0.05 * xG_Expected_per90_ratio -
      0.05 * CrdY_per90_ratio -
      0.05 * CrdR_per90_ratio
    WHEN {{ Poste_simplifie }} = 'Milieu relayeur' THEN
      0.15 * Cmp_Passes_per90_ratio +
      0.15 * PrgP_Passes_per90_ratio +
      0.10 * Carries_Carries_per90_ratio +
      0.10 * Succ_Take_Ons_per90_ratio +
      0.10 * Int_per90_ratio +
      0.10 * Tkl_per90_ratio +
      0.10 * xAG_Expected_per90_ratio +
      0.05 * GCA_SCA_per90_ratio -
      0.05 * CrdY_per90_ratio -
      0.05 * CrdR_per90_ratio
    WHEN  {{ Poste_simplifie }} = 'Milieu defensif' THEN
      0.20 * Tkl_per90_ratio +
      0.20 * Int_per90_ratio +
      0.15 * Blocks_per90_ratio +
      0.10 * Cmp_Passes_per90_ratio +
      0.10 * PrgP_Passes_per90_ratio +
      0.10 * Touches_per90_ratio -
      0.10 * CrdY_per90_ratio -
      0.05 * CrdR_per90_ratio
    WHEN {{ Poste_simplifie }} = 'Defenseur central' THEN
      0.20 * Tkl_per90_ratio +
      0.20 * Int_per90_ratio +
      0.20 * Blocks_per90_ratio +
      0.15 * PrgP_Passes_per90_ratio +
      0.10 * Touches_per90_ratio +
      0.10 * Cmp_Passes_per90_ratio -
      0.10 * CrdY_per90_ratio -
      0.10 * CrdR_per90_ratio
    WHEN {{ Poste_simplifie }} = 'Lateral' THEN
      0.15 * Tkl_per90_ratio +
      0.15 * Int_per90_ratio +
      0.10 * Blocks_per90_ratio +
      0.05 * Touches_per90_ratio +
      0.15 * PrgP_Passes_per90_ratio +
      0.10 * Succ_Take_Ons_per90_ratio +
      0.10 * xAG_Expected_per90_ratio +
      0.05 * xG_Expected_per90_ratio -
      0.05 * CrdY_per90_ratio -
      0.05 * CrdR_per90_ratio
  END
{% endmacro %}