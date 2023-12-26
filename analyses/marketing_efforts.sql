{%set product_groups = {'winter':'Hàng đông','summer':'Hàng hè','casual':'Quanh năm'} %}

WITH revenue AS (
    SELECT
        DATE(transaction_date) t_date,
        COUNT(
            DISTINCT transaction_id
        ) AS num_invoice,
        SUM(subTotal) AS items_value,
        SUM(quantity) AS items_qty,
        AVG(price) AS avg_price,
        AVG(discount_ratio) AS avg_discount_ratio,
    {%for name,group in product_groups.items() %}
        COUNT(
            DISTINCT case when p.product_group='{{group}}' then transaction_id end
        ) AS {{name}}_num_invoice,
        SUM( case when p.product_group='{{group}}' then subTotal end) AS {{name}}_items_value,
        SUM( case when p.product_group='{{group}}' then quantity end) AS {{name}}_items_qty,
        AVG(case when p.product_group='{{group}}' then price end) AS {{name}}_avg_price,
        AVG(case when p.product_group='{{group}}' then discount_ratio end) AS {{name}}_avg_discount_ratio,
    {%endfor%}
    FROM
        {{ ref("revenue_items") }}
        r
        LEFT JOIN {{ ref("stg_kiotviet__branches") }}
        b
        ON r.branch_id = b.branch_id
        left join {{ref("stg_kiotviet__products")}} p on r.product_id = p.product_id
    WHERE
        transaction_date >= '2023-06-01'
        AND b.branch_name LIKE '5S%'
        AND r.transaction_type = 'invoice'
        and r.price >0
        and p.productline <>'NGUYÊN PHỤ LIỆU'
    GROUP BY
        1
),
ads_spend AS (
    SELECT
        date_start,
        SUM(spend) spend,
        SUM(impressions) impressions
    FROM
        {{ ref("facebook_performance") }}
    WHERE
        date_start >= '2023-06-01'
    GROUP BY
        1
),
web_metrics AS (
    SELECT
        session_date,
        direct,
        organic_search,
    FROM
        (
            SELECT
                session_date,
                channel_grouping,
                COUNT(
                    DISTINCT session_id
                ) num_sessions,
                from {{ ref("analytics_sessions") }}
            GROUP BY
                1,
                2
        ) 
        pivot(
            sum(num_sessions) for channel_grouping IN (
                "Direct" AS direct, 
                "Organic Search" AS organic_search
        )
)
),
sms as (
    select 
        * 
    from {{ref("sms__by__day")}}
)



SELECT
    r.*,
    ads.* EXCEPT(date_start),
    web.* EXCEPT(session_date),
    sms.* except(sent_date),
FROM
    revenue r
    LEFT JOIN ads_spend ads
    ON r.t_date = ads.date_start
    LEFT JOIN web_metrics web
    ON r.t_date = web.session_date
    left join sms
    on r.t_date = sms.sent_date
