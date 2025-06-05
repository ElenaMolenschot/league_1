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

