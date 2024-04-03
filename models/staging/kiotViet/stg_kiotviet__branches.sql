{{
  config(
    tags=['view', 'dimension','kiotviet']
  )
}}
SELECT
    b.id as branch_id,
    b.branchName as branch_name,
    b.address,
    b.wardName ward_name,
    regexp_replace(b.contactNumber,r'\.|\,|\s','') as contact_number,
    b.email,
    b.createdDate as created_Date,
    b.modifiedDate as modified_date,
    {# coalesce(r.region,"(Chưa phân loại)") region, #}
    {# coalesce(r.province,"(Chưa phân loại)") province, #}
FROM
    {{ ref('base_kiotViet__branches') }} b
  {# left join {{ref('stg_gsheet__regions')}} r on lower(b.branchName) = lower(r.branch_name) #}