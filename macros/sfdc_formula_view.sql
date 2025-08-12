{%- macro sfdc_formula_view(source_table, source_name='salesforce', materialization='view', using_quoted_identifiers=False, full_statement_version=true, reserved_table_name=none, fields_to_include=none) -%}

{{
    config(
        materialized = materialization
    )
}}

{% if not full_statement_version %}
    {{ exceptions.warn("\nERROR: The full_statement_version=false, reserved_table_name, and fields_to_include parameters are no longer supported. Please update your " ~ this.identifier|upper ~ " model to remove these parameters.\n") }}
    See_full_model_error_in_log

{% else %}

    {% if using_quoted_identifiers %}
        {%- set quoted_col = (
            '"MODEL"' if target.type in ('snowflake') else
            '"model"' if target.type in ('postgres', 'redshift', 'snowflake') else
            '`model`'
        ) -%}

        {%- set quoted_where = (
            "\"OBJECT\" = '" if target.type in ('snowflake') else
            "\"object\" = '" if target.type in ('postgres', 'redshift') else
            "`object` = '"
        ) ~ source_table ~ "'" -%}

        {%- set table_results = dbt_utils.get_column_values(
            table=source(source_name, 'fivetran_formula_model'),
            column=quoted_col,
            where=quoted_where
        ) -%}

    {% else %}
        {%- set table_results = dbt_utils.get_column_values(
            table=source(source_name, 'fivetran_formula_model'),
            column='model',
            where="object = '" ~ source_table ~ "'"
        ) -%}
    {% endif %}

    {% if table_results | length == 0 %}
        {{ exceptions.warn("\nWARNING: No formula model found in fivetran_formula_model for object '" ~ source_table ~ "'. Defaulting to SELECT * FROM " ~ source(source_name, source_table) ~ "\n") }}
        select * from {{ source(source_name, source_table) }}
    {% else %}
        {{ table_results[0] }}
    {% endif %}

{% endif %}

{%- endmacro -%}
