{% macro cast_all_as_string(source_name, table_name, add_source_column=true) %}
    {% set relation = source(source_name, table_name) %}
    {% set cols = adapter.get_columns_in_relation(relation) %}

    {% set casted_cols = [] %}
    {% for col in cols %}
        {% do casted_cols.append("SAFE_CAST(" ~ col.name ~ " AS STRING) AS " ~ col.name) %}
    {% endfor %}

    {% set source_tag = "'" ~ table_name ~ "' AS source_ligue" if add_source_column else "" %}
    
    SELECT {{ casted_cols | join(', ') }}{% if add_source_column %}, {{ source_tag }}{% endif %} FROM {{ relation }}
{% endmacro %}
