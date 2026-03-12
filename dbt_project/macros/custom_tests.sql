-- =============================================================================
-- macros/tests/test_no_future_dates.sql
--
-- PURPOSE: Custom generic test — reusable across any model.
-- Fails if a timestamp column contains dates in the future.
-- Used to catch data pipeline bugs where timestamps are wrong.
--
-- USAGE in schema.yml:
--   - name: created_at
--     tests:
--       - no_future_dates
-- =============================================================================

{% test no_future_dates(model, column_name) %}

select
    {{ column_name }},
    count(*) as num_records
from {{ model }}
where {{ column_name }} > current_timestamp
group by 1
having count(*) > 0

{% endtest %}


-- =============================================================================
-- macros/tests/test_positive_values.sql
--
-- PURPOSE: Custom generic test — ensures numeric column is always > 0
-- Useful for amounts, counts, rates
--
-- USAGE in schema.yml:
--   - name: mrr
--     tests:
--       - positive_values
-- =============================================================================

{% test positive_values(model, column_name) %}

select {{ column_name }}
from {{ model }}
where {{ column_name }} <= 0

{% endtest %}


-- =============================================================================
-- macros/generate_schema_name.sql
--
-- PURPOSE: Override dbt's default schema naming behavior.
-- By default dbt appends your target schema to model schemas.
-- This macro gives us clean schema names in both dev and prod.
--
-- Dev:  dbt_john_staging, dbt_john_marts, etc.
-- Prod: staging, marts, reporting, etc.
-- =============================================================================

{% macro generate_schema_name(custom_schema_name, node) -%}

    {%- set default_schema = target.schema -%}

    {%- if custom_schema_name is none -%}
        {{ default_schema }}

    {%- elif target.name == 'prod' -%}
        -- In prod: use the custom schema name directly (no prefix)
        {{ custom_schema_name | trim }}

    {%- else -%}
        -- In dev: prefix with personal schema so devs don't collide
        {{ default_schema }}_{{ custom_schema_name | trim }}

    {%- endif -%}

{%- endmacro %}
