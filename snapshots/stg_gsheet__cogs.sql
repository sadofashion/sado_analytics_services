{% snapshot stg_gsheet__cogs %}

    {{
        config(
          target_schema='dbt_'+target.name,
          strategy='check',
          unique_key='product_code',
          check_cols=['inventory_value_per_unit'],
        )
    }}

    select 
    product_code , 
    inventory_value_per_unit,
    from {{ source('gSheet', 'cogs') }}

{% endsnapshot %}