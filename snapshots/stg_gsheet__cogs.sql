{% snapshot stg_gsheet__cogs %}

    {{
        config(
          target_schema='dbt_'+target.name,
          strategy='check',
          unique_key='product_code',
          check_cols=['inventory_value_per_unit'],
          post_hook=['update_cogs_valid_date()']
        )
    }}

    select 
    product_code , 
    inventory_value_per_unit,
    from {{ source('gSheet', 'cogs') }}
    where regexp_extract(_FILE_NAME,r'cogs__(\d{6})') = format_date('%Y%m',current_date)
    

{% endsnapshot %}