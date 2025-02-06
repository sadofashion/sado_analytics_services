{% macro delete_outdated_tables(schema) %} 
  {% if (schema is not string and schema is not iterable) or schema is mapping or schema|length <= 0 %}
    {% do exceptions.raise_compiler_error('"schema" must be a string or a list') %}
  {% endif %}

  {% call statement('get_outdated_tables', fetch_result=True) %}
    select _current.schema_name,
           _current.ref_name,
           _current.ref_type
    from (
      select table_schema as schema_name, 
             table_name  as ref_name, 
             "TABLE" as ref_type
      from `{{target.schema}}.INFORMATION_SCHEMA.TABLES` pt
      where table_type = 'BASE TABLE'
      and table_schema in (
        {%- if schema is iterable and (var is not string and var is not mapping) -%}
          {%- for s in schema -%}
            '{{ s }}'{% if not loop.last %},{% endif %}
          {%- endfor -%}
        {%- elif schema is string -%}
          '{{ schema }}'
        {%- endif -%}
      )) as _current
    left join (
      select * from unnest([
        STRUCT<schema_name string, ref_name string>
      {%- for node in graph.nodes.values() | selectattr("resource_type", "equalto", "model") | list
                    + graph.nodes.values() | selectattr("resource_type", "equalto", "seed")  | list %} 
        ('{{node.schema}}', '{{node.name}}'){% if not loop.last %},{% endif %}
      {%- endfor %}
    ])) as _desired on _desired.schema_name = _current.schema_name
                                        and _desired.ref_name    = _current.ref_name
    where _desired.ref_name is null
  {% endcall %}

  {%- for to_delete in load_result('get_outdated_tables')['data'] %} 
    {% call statement() -%}
      {% do log('dropping ' ~ to_delete[2] ~ ' "' ~ to_delete[0] ~ '.' ~ to_delete[1], info=true) %}
      drop {{ to_delete[2] }} if exists `{{ to_delete[0] }}`.`{{ to_delete[1] }}` ;
    {%- endcall %}
  {%- endfor %}

{% endmacro %}