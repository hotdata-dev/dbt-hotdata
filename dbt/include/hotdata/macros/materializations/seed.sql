{#
  Seeds load like everything else on Hotdata: the parsed CSV (agate) becomes
  an Arrow table (numbers stay exact — integers and decimals, not floats),
  is written as parquet, and a native `replace` load applies it. There is no
  INSERT surface, so `column_types:` overrides are applied as Arrow casts
  before the load rather than as DDL column types.
#}
{% materialization seed, adapter='hotdata' %}

  {%- set identifier = model['alias'] -%}
  {%- set target_relation = api.Relation.create(
        identifier=identifier, schema=schema, database=database, type='table') -%}

  {%- set agate_table = load_agate_table() -%}
  {%- do store_result('agate_table', response='OK', agate_table=agate_table) -%}

  {{ run_hooks(pre_hooks, inside_transaction=False) }}
  {{ run_hooks(pre_hooks, inside_transaction=True) }}

  {%- set result = adapter.load_seed(target_relation, agate_table, config.get('column_types', {})) -%}
  {% call noop_statement('main', result['message'], 'LOAD', result['rows']) -%}
    -- seed {{ identifier }}
  {%- endcall %}

  {{ run_hooks(post_hooks, inside_transaction=True) }}
  {% do adapter.commit() %}
  {{ run_hooks(post_hooks, inside_transaction=False) }}

  {% do persist_docs(target_relation, model) %}

  {{ return({'relations': [target_relation]}) }}

{% endmaterialization %}
