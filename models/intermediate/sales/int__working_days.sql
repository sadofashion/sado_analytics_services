select
    {{dbt_utils.generate_surrogate_key(['b.branch_id', 'c.date'])}} as branch_working_day_id,
    b.* except(phone,email,province_code, region_code, frontage, area_sqm),
    c.* except(start_of_week,end_of_week),
    case when date_diff(c.date,b.opening_day,day) <= 10 then 'Khai trương' else p.promotion end as promotion,
from {{ ref("dim__branches") }} b
left join {{ ref("calendar") }} c on coalesce(b.opening_day,'2023-01-01') <= c.date and (b.close_date >= c.date or b.close_date is null)
left join {{ ref("promotions") }} p on c.date>=p.start and c.date<=p.end and p.location = b.model_type
where 1=1
{# and b.opening_day is not null #}
and b.channel not in ('Kho & CH khác Kiotviet')
and c.date <= current_date()
{# AND b.branch_id NOT IN (1000087891) #}
{# and b.asm_name is not null #}