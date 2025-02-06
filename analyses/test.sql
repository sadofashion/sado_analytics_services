{% set col = [null,'a']%}
{% for i in col %}
select '{{i}}' as col_val,
case when '{{i}}' is null then 'null val' else 'not null' end as result
{{'union all' if not loop.last}}
{%endfor%}
