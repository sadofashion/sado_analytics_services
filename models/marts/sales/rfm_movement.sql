{{ config(
  materialized = 'incremental',
  partition_by ={ 'field': 'start_of_month',
  'data_type': 'date',
  'granularity': 'month' },
  incremental_strategy = 'merge',
  unique_key = ['customer_id', 'start_of_month'],
  on_schema_change = 'sync_all_columns',
  tags = ['incremental','table', 'fact', 'kiotviet']
) }}

{% set rfm_groups ={ 
  "Champions": ['555','554','544','545','454','455','445'],
  "Loyal" :['543','444','435','355','354','345','344','335'],
  "Potential Loyalists": ['553', '551', '552', '541', '542', '533', '532', '531', '452', '451', '442', '441', '431', '453', '433', '432', '423', '353', '352', '351', '342', '341', '333', '323'],
  'New Customers' :['512', '511', '422', '421','412', '411', '311'],
  'Promising' :['525', '524', '523', '522', '521', '515', '514', '513', '425', '424', '413', '414', '415', '315', '314', '313'],
  'Need Attention' :['535', '534', '443', '434', '343', '334', '325', '324'],
  'About To Sleep' :[ '331', '321', '312', '221', '213', '231', '241', '251'],
  'At Risk' :[ '255', '254', '245', '244', '253', '252', '243', '242', '235', '234', '225', '224', '153', '152', '145', '143', '142', '135', '134', '133', '125', '124'],
  'Cannot Lose Them' :[ '155', '154', '144', '214', '215', '115', '114', '113'],
  'Hibernating customers' :[ '332', '322', '233', '232', '223', '222', '132', '123', '122', '212', '211'],
  'Lost customers' :[ '111', '112', '121', '131', '141', '151'] 
} 
%}
WITH calendar AS (

  SELECT
    DISTINCT start_of_month,
  FROM
    {{ ref('calendar') }}
),
source AS (
  SELECT
    customer_id,
    DATE(transaction_date) transaction_date,
    DATE_TRUNC(DATE(transaction_date), MONTH) transaction_month,
    SUM(total) total,
    COUNT(
      DISTINCT CASE
        WHEN transaction_type = 'invoice' THEN DATE_TRUNC(
          transaction_date,
          DAY
        )
      END
    ) num_transactions,
  FROM
    {{ ref('revenue') }}
  WHERE
    customer_id IS NOT NULL
    {# and total <> 0 #}
  GROUP BY
    1,
    2,
    3
),
aggregated_and_cross_join AS (
  SELECT
    calendar.start_of_month,
    source.customer_id,
    SUM(
      CASE
        WHEN calendar.start_of_month = source.transaction_month THEN total
      END
    ) total,
    SUM(
      CASE
        WHEN calendar.start_of_month = source.transaction_month THEN num_transactions
      END
    ) num_transactions,
    MAX(
      CASE
        WHEN calendar.start_of_month = source.transaction_month THEN transaction_date
      END
    ) transaction_date
  FROM
    calendar
    CROSS JOIN source
  WHERE
    1 = 1
    AND calendar.start_of_month <= CURRENT_DATE()
  GROUP BY
    1,
    2
),
aggregated_cumulative AS (
  SELECT
    start_of_month,
    customer_id,
    MIN(transaction_date) over w3 AS first_purchase,
    COALESCE(MAX(transaction_date) over w4, MAX(transaction_date) over w5) AS last_purchase,
    coalesce(safe_divide(SUM(total) over w1,SUM(num_transactions) over w1),0) AS monetary,
    COALESCE(SUM(num_transactions) over w1, 0) AS frequency,
    date_diff(
      COALESCE(MAX(transaction_date) over w2, case when LAST_DAY(start_of_month, MONTH) < current_date() then LAST_DAY(start_of_month, MONTH) else current_date() end  ),
      COALESCE(MAX(transaction_date) over w4, MAX(transaction_date) over w5),
      DAY) AS recency
      FROM
        aggregated_and_cross_join 
        window w1 AS (
          PARTITION BY customer_id
          ORDER BY
            unix_date(LAST_DAY(start_of_month, MONTH)) asc RANGE BETWEEN 365 preceding
            AND CURRENT ROW
        ),
        w2 AS (
          PARTITION BY customer_id,
          start_of_month
          ORDER BY
            unix_date(start_of_month) ASC RANGE BETWEEN unbounded preceding
            AND 1 preceding
        ),
        w4 AS (
          PARTITION BY customer_id,
          start_of_month
          ORDER BY
            unix_date(start_of_month) ASC RANGE BETWEEN unbounded preceding
            AND CURRENT ROW
        ),
        w5 AS (
          PARTITION BY customer_id
          ORDER BY
            unix_date(start_of_month) ASC RANGE BETWEEN unbounded preceding
            AND 1 preceding
        ),
        w3 AS (
          PARTITION BY customer_id
        )
    ),
    scoring AS (
      SELECT
        customer_id,
        start_of_month,
        first_purchase,
        last_purchase,
        recency,
        monetary,
        frequency,
        NTILE(5) over (
          PARTITION BY start_of_month
          ORDER BY
            recency DESC
        ) AS recency_score,
        CASE
          WHEN frequency > 0 THEN NTILE(5) over (
            PARTITION BY start_of_month
            ,(
              CASE
                WHEN monetary > 0 THEN "purchase"
                ELSE "notpurchase"
              END
            )
            ORDER BY
              frequency ASC
          )
          ELSE 1
        END AS frequency_score,
        CASE
          WHEN monetary > 0 THEN NTILE(5) over (
            PARTITION BY start_of_month
            ,
            (
              CASE
                WHEN monetary > 0 THEN "purchase"
                ELSE "notpurchase"
              END
            )
            ORDER BY
              monetary ASC
          )
          ELSE 1
        END AS monetary_score
      FROM
        aggregated_cumulative
      WHERE
        start_of_month >= DATE_TRUNC(
          first_purchase,
          MONTH
        )
    ),
    last_branch AS (
      SELECT
        DISTINCT customer_id,
        DATE(transaction_date) transaction_date,
        DATE_TRUNC(DATE(transaction_date), MONTH) AS transaction_month,
        FIRST_VALUE(branch_id) over (
          PARTITION BY customer_id,
          DATE_TRUNC(DATE(transaction_date), MONTH)
          ORDER BY
            transaction_date DESC rows BETWEEN unbounded preceding
            AND unbounded following) AS last_purchase_branch,
          FROM
            {{ ref('revenue') }}
          WHERE
            customer_id IS NOT NULL
        ),
      final as (
        SELECT
        DISTINCT scoring.*,
        CONCAT(
          recency_score,
          frequency_score,
          monetary_score
        ) score_concat,
        CASE
        {%for key,values in rfm_groups.items() %}
          WHEN CONCAT(
            recency_score,
            frequency_score,
            monetary_score
          ) in ('{{values | join("', '")}}') then '{{key}}'
          {%endfor%}
        END AS segment,
        CASE
          WHEN DATE_TRUNC(
            first_purchase,
            MONTH
          ) = start_of_month THEN "Khách mới"
          WHEN recency > 360 THEN '1 năm chưa quay lại'
          WHEN recency > 180 THEN '6 tháng chưa quay lại'
          WHEN recency > 90 THEN '3 tháng chưa quay lại'
          WHEN recency > 31 THEN '1 tháng chưa quay lại'
          ELSE "Khách quay lại"
        END AS recency_type,
        last_branch.last_purchase_branch,
      FROM
        scoring
        LEFT JOIN last_branch
        ON scoring.customer_id = last_branch.customer_id
        AND last_branch.transaction_month = scoring.start_of_month

{% if is_incremental() %}
WHERE
  DATE(start_of_month) >= date_add(DATE(_dbt_max_partition), interval -1 month)
{% endif %}
),
previous as (
select * except(last_purchase_branch),
{# last_purchase_branch, #}
first_value(last_purchase_branch ignore nulls) over (
          partition by customer_id 
          order by start_of_month desc 
          rows between current row and unbounded following) last_purchase_branch,
coalesce(lag(segment) over (partition by customer_id order by start_of_month asc),'First-time Purchaser') as previous_segment
from final
)

select * from previous 
{% if is_incremental() %}
WHERE
  DATE(start_of_month) >= DATE(_dbt_max_partition)
{% endif %}