{% macro on_run_start() %}
  {# Lista de schemas que seu projeto usa #}
  {% set schemas = ['raw','silver','gold'] %}

  {% for s in schemas %}
    {% do adapter.create_schema(
         api.Relation.create(database=target.database, schema=s)
       )
    %}
  {% endfor %}
{% endmacro %}
