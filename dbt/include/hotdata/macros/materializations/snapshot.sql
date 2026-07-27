{#
  Snapshots (SCD2 change history) need in-place row versioning the managed
  load API does not provide. Fail up front with a clear message, matching the
  hotdata-dlt-destination behavior for scd2.
#}
{% materialization snapshot, adapter='hotdata' %}
  {% do exceptions.raise_compiler_error(
    "dbt snapshots are not supported on Hotdata: merges update rows in place, so "
    "past versions are not kept. Keep history by loading immutable extracts with an "
    "incremental model (strategy 'append') instead."
  ) %}
{% endmaterialization %}
