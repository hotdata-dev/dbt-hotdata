{#
  Incremental materialization. Strategies:

  * append (default) — run the model's SELECT, native `append` load.
  * merge — requires `unique_key`; native `upsert` load matched per-load on
    the key (updates matches, inserts the rest), no full-table read.

  First run and --full-refresh take the table path (`replace`). Schema
  changes are additive server-side: new columns in the model's result appear
  on the next load without touching existing data, so `on_schema_change` has
  nothing to do and is ignored (a non-default setting warns).
#}
{% materialization incremental, adapter='hotdata' %}

  {%- set target_relation = this.incorporate(type='table') -%}
  {%- set existing_relation = load_cached_relation(this) -%}

  {%- set strategy = config.get('incremental_strategy') or 'append' -%}
  {%- set unique_key = config.get('unique_key') -%}
  {%- if strategy not in ['append', 'merge'] -%}
    {% do exceptions.raise_compiler_error(
      "incremental_strategy '" ~ strategy ~ "' is not supported on Hotdata — use "
      "'append' (default) or 'merge' (requires unique_key; loads as a native upsert)"
    ) %}
  {%- endif -%}
  {%- if strategy == 'merge' and not unique_key -%}
    {% do exceptions.raise_compiler_error(
      "the 'merge' incremental strategy on Hotdata requires a unique_key (rows are "
      "matched and upserted server-side by that key)"
    ) %}
  {%- endif -%}

  {%- set on_schema_change = config.get('on_schema_change', 'ignore') -%}
  {%- if on_schema_change != 'ignore' -%}
    {% do exceptions.warn(
      "on_schema_change: '" ~ on_schema_change ~ "' is ignored on Hotdata — loads add "
      "new columns and widen types additively on their own"
    ) %}
  {%- endif -%}

  {%- if existing_relation is none or should_full_refresh() -%}
    {%- set mode = 'replace' -%}
  {%- elif strategy == 'merge' -%}
    {%- set mode = 'upsert' -%}
  {%- else -%}
    {%- set mode = 'append' -%}
  {%- endif -%}

  {{ run_hooks(pre_hooks, inside_transaction=False) }}
  {{ run_hooks(pre_hooks, inside_transaction=True) }}

  {%- set result = adapter.create_table_from_query(target_relation, compiled_code, mode, unique_key) -%}
  {% call noop_statement('main', result['message'], 'LOAD', result['rows']) -%}
    {{ compiled_code }}
  {%- endcall %}

  {{ run_hooks(post_hooks, inside_transaction=True) }}
  {% do adapter.commit() %}
  {{ run_hooks(post_hooks, inside_transaction=False) }}

  {% do persist_docs(target_relation, model) %}

  {{ return({'relations': [target_relation]}) }}

{% endmaterialization %}
