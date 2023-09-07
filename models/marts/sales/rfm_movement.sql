WITH calendar as (
  select distinct start_of_month, 
  from {{ref('calendar')}}
),
source AS (
  SELECT
    customer_id,
    DATE(transaction_date) transaction_date,
    DATE_TRUNC(
      DATE(transaction_date),
      MONTH
    ) transaction_month,
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
    where customer_id is not null
  GROUP BY
    1,
    2,
    3
),
aggregated_and_cross_join as (
select 
  calendar.start_of_month,
  source.customer_id,
  sum(case when calendar.start_of_month= source.transaction_month then total end) total,
  sum(case when calendar.start_of_month= source.transaction_month then num_transactions end) num_transactions,
  max(case when calendar.start_of_month= source.transaction_month then transaction_date end) transaction_date
from calendar
cross join source
where 1=1
  and calendar.start_of_month <= current_date()
group by 1,2
),

aggregated_cumulative as (
  select 
    start_of_month,
    customer_id,
    min(transaction_date) over w3 as first_purchase,
    max(transaction_date) over w2 as last_purchase,
    sum(total) over w1 as monetary,
    sum(num_transactions) over w1 as frequency,
    date_diff(coalesce(min(transaction_date) over w3,last_day(start_of_month,month)), max(transaction_date) over w2,day) as recency
  from aggregated_and_cross_join
  window w1 as (
    PARTITION by customer_id order by unix_date(start_of_month) desc
    range between 93 preceding and current row
  ),
  w2 as (
    PARTITION by customer_id order by unix_date(start_of_month) asc range between unbounded preceding and 1 preceding
  ),
  w3 as (
    PARTITION by customer_id,start_of_month
  )
),

scoring as (
  SELECT
  customer_id,
  start_of_month,
  first_purchase,
  last_purchase,
  recency,
  monetary,
  frequency,
  NTILE(5) OVER (PARTITION by start_of_month ORDER BY recency desc ) AS recency_score,
  NTILE(5) OVER (PARTITION by start_of_month ORDER BY frequency asc ) AS frequency_score,
  NTILE(5) OVER (PARTITION by start_of_month ORDER BY monetary asc ) AS monetary_score
FROM
  aggregated_cumulative
  where
  start_of_month >= date_trunc(first_purchase,month)
  )

  select *,
  concat(recency_score,frequency_score,monetary_score) score_concat,
  case when concat(recency_score,frequency_score,monetary_score) in ('555', '554', '544', '545', '454', '455', '445') then 'Champions'
  when concat(recency_score,frequency_score,monetary_score) in ('543','444','435','355','354','345','344','335') then 'Loyal'
  when concat(recency_score,frequency_score,monetary_score) in ('553','551','552','541','542','533','532','531','452','451','442','441','431','453','433','432','423','353','352','351','342','341','333','323') then 'Potential Loyalists'
  when concat(recency_score,frequency_score,monetary_score) in ('512','511','422',' 421 412','411','311') then 'New Customers'
  when concat(recency_score,frequency_score,monetary_score) in ('525','524','523','522','521','515','514','513','425','424','413','414','415','315','314','313') then 'Promising'
  when concat(recency_score,frequency_score,monetary_score) in ('535','534','443','434','343','334','325','324') then 'Need Attention'
  when concat(recency_score,frequency_score,monetary_score) in ('331','321','312','221','213','231','241','251') then 'About To Sleep'
  when concat(recency_score,frequency_score,monetary_score) in ('255','254','245','244','253','252','243','242','235','234','225','224','153','152','145','143','142','135','134','133','125','124') then 'At Risk'
  when concat(recency_score,frequency_score,monetary_score) in ('155','154','144','214','215','115','114','113') then 'Cannot Lose Them'
  when concat(recency_score,frequency_score,monetary_score) in ('332','322','233','232','223','222','132','123','122','212','211') then 'Hibernating customers'
  when concat(recency_score,frequency_score,monetary_score) in ('111','112','121','131','141','151') then 'Lost customers'
  end as segment
  from scoring
