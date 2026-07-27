{#
  Hotdata has no DDL surface: metadata operations are implemented in Python on
  the adapter (list_relations, get_columns, drop/truncate, catalog), and
  materializations load query results through the managed-table API. The
  macros here exist so that any default-macro dispatch path lands somewhere
  honest instead of emitting SQL the engine cannot run.
#}

{% macro hotdata__current_timestamp() -%}
  now()
{%- endmacro %}

{% macro hotdata__create_schema(relation) -%}
  {% do adapter.create_schema(relation) %}
{%- endmacro %}

{% macro hotdata__drop_schema(relation) -%}
  {% do adapter.drop_schema(relation) %}
{%- endmacro %}

{% macro hotdata__drop_relation(relation) -%}
  {% do adapter.drop_relation(relation) %}
{%- endmacro %}

{% macro hotdata__truncate_relation(relation) -%}
  {% do adapter.truncate_relation(relation) %}
{%- endmacro %}

{% macro hotdata__rename_relation(from_relation, to_relation) -%}
  {% do adapter.rename_relation(from_relation, to_relation) %}
{%- endmacro %}

{% macro hotdata__create_table_as(temporary, relation, compiled_code, language='sql') -%}
  {#-
    Dispatched by store_failures and other default paths. The load happens as
    a side effect via the adapter; the returned statement is a cheap no-op so
    callers that wrap this in `statement()` still have something to execute.
  -#}
  {%- if language != 'sql' -%}
    {% do exceptions.raise_compiler_error("Python models are not supported on Hotdata") %}
  {%- endif -%}
  {#-
    temporary=True builds a REAL table under the temp name: Hotdata has no
    session scope, and every dbt-core caller that asks for a temp table
    (e.g. the unit-test materialization) drops it itself when done.
  -#}
  {% do adapter.create_table_from_query(relation, compiled_code, 'replace') %}
  select 1 as loaded
{%- endmacro %}

{% macro hotdata__create_view_as(relation, sql) -%}
  {% do exceptions.raise_compiler_error(
    "Hotdata has no views. Materialize this model as a table instead — in "
    "dbt_project.yml:\n\nmodels:\n  " ~ project_name ~ ":\n    +materialized: table"
  ) %}
{%- endmacro %}

{% macro hotdata__apply_grants(relation, grant_config, should_revoke=True) -%}
  {%- if grant_config -%}
    {% do exceptions.warn("Hotdata does not support grants; the grants config on " ~ relation ~ " is ignored (access is governed by workspace API keys)") %}
  {%- endif -%}
{%- endmacro %}

{% macro hotdata__persist_docs(relation, model, for_relation, for_columns) -%}
  {# No comment DDL; docs live in the dbt docs site only. #}
{%- endmacro %}

{% macro hotdata__make_temp_relation(base_relation, suffix) -%}
  {#-
    Temp names carry the invocation id so a name is never dropped and then
    redeclared: deleting a managed table leaves its name briefly (sometimes
    persistently) in a state where re-declaring 409s "already exists" while
    loads 404 — reusing names across runs would land squarely on it.
  -#}
  {%- set temp_identifier = base_relation.identifier ~ suffix ~ '_' ~ invocation_id.replace('-', '')[:8] -%}
  {{ return(base_relation.incorporate(path={"identifier": temp_identifier})) }}
{%- endmacro %}

{% macro hotdata__get_columns_in_relation(relation) -%}
  {{ return(adapter.get_columns_in_relation(relation)) }}
{%- endmacro %}

{% macro hotdata__list_schemas(database) -%}
  {{ return(adapter.list_schemas(database)) }}
{%- endmacro %}

{% macro hotdata__check_schema_exists(information_schema, schema) -%}
  {{ return(adapter.check_schema_exists(information_schema.database, schema)) }}
{%- endmacro %}

{% macro hotdata__alter_column_type(relation, column_name, new_column_type) -%}
  {% do exceptions.raise_compiler_error("Hotdata does not support ALTER COLUMN; loads widen types additively on their own") %}
{%- endmacro %}

{% macro hotdata__alter_relation_add_remove_columns(relation, add_columns, remove_columns) -%}
  {% do exceptions.raise_compiler_error("Hotdata does not support ALTER TABLE; new columns appear when the next load includes them") %}
{%- endmacro %}
