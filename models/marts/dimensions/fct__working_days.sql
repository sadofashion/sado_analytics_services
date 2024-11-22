{{
  config(
    materialized = 'view',
    )
}}

select
    {{dbt_utils.generate_surrogate_key(['b.branch_id', 'c.date'])}} as branch_working_day_id,
    b.* except(phone,email,province_code, region_code, frontage, area_sqm),
    c.* except(start_of_week,end_of_week),
    case when date_diff(c.date,b.opening_day,day) <= 10 then 'Khai trương' else p.promotion end as promotion,
from {{ ref("dim__branches") }} b
left join {{ ref("calendar") }} c on b.opening_day <= c.date and (b.close_date >= c.date or b.close_date is null)
left join {{ ref("promotions") }} p on c.date>=p.start and c.date<=p.end and p.location = b.model_type
where b.opening_day is not null
and b.channel not in ('Kho & CH khác Kiotviet',"Điểm xả")
and c.date <= current_date()