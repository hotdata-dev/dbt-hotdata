{#
  Table materialization, Hotdata-style ("Chain"): the model's compiled SELECT
  runs server-side, the result comes back as Arrow, and a native `replace`
  load applies it to the managed table. No temp table, no rename swap — the
  engine replaces the table's contents in one load. First runs and rebuilds
  take the same path (the table is declared on the database when missing).
#}
{% materialization table, adapter='hotdata' %}

  {%- set target_relation = this.incorporate(type='table') -%}

  {{ run_hooks(pre_hooks, inside_transaction=False) }}
  {{ run_hooks(pre_hooks, inside_transaction=True) }}

  {%- set result = adapter.create_table_from_query(target_relation, compiled_code, 'replace') -%}
  {% call noop_statement('main', result['message'], 'LOAD', result['rows']) -%}
    {{ compiled_code }}
  {%- endcall %}

  {{ run_hooks(post_hooks, inside_transaction=True) }}
  {% do adapter.commit() %}
  {{ run_hooks(post_hooks, inside_transaction=False) }}

  {% do persist_docs(target_relation, model) %}

  {{ return({'relations': [target_relation]}) }}

{% endmaterialization %}
