{#
  Hotdata has no views, and dbt's default materialization is `view` — so this
  fails up front with the fix, rather than mid-run with an engine error.
#}
{% materialization view, adapter='hotdata' %}
  {% do exceptions.raise_compiler_error(
    "Hotdata has no views (models default to materialized: view). Materialize as a "
    "table instead — either per model with {{ config(materialized='table') }}, or for "
    "the whole project in dbt_project.yml:\n\nmodels:\n  " ~ project_name ~
    ":\n    +materialized: table\n\nModels that should not be built as tables can use "
    "materialized: ephemeral (inlined into their consumers)."
  ) %}
{% endmaterialization %}
