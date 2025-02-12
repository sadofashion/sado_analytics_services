{{
  config(
    materialized = 'table',
    tags = [
      'staging',
      'gsheet',
      'fact',
      'daily'
    ]
    )
}}

with service_claims as (
    select 
        parse_date('%e/%m/%Y', resolve_date) as resolve_date,
        parse_date('%e/%m/%Y', date) as claim_date,
        branch_name,
        {# sales_rep, #}
        claim_code,
        safe_cast(claim_no as int) claim_no,
        safe_cast(minus_points as int) minus_points,
        claim_channel,
        status,
        claim_type as claim_category,
        'Dịch vụ' as  type,
        cast(null as string) as product_line_code,
        cast(null as string) as design_code,
        claim_reason as claim_detail,
    from {{ source('gSheet', 'service_claim') }}
    where 1=1
        and date is not null
),

product_claims as (
    select 
        parse_date('%e/%m/%Y', coalesce(exchange_date,date)) as resolve_date,
        parse_date('%e/%m/%Y', date) as claim_date,
        branch_name,
        {# sales_rep, #}
        claim_type,
        cast(null as int) as claim_no,
        cast(null as int) minus_points,
        claim_channel,
        status,
        claim_type as claim_category,
        'Sản phẩm' as  type,
        product_line_code,
        design_code,
        claim_detail,
    from {{ source('gSheet', 'product_claim') }}
    where 1=1
        and date is not null
)


select 
* from service_claims
union all
select
* from product_claims