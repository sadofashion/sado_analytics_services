{% snapshot offline_ads_pages %}
  {% set target_schema = target.name + '_snapshots' %}
  {{ config(
    target_schema = target_schema,
    strategy = 'check',
    unique_key = 'branch_id',
    check_cols = ['fb_ads_page','branch_id']
  ) }}

  SELECT
    A.branch_id,
    A.asm_name,
    A.phone AS asm_phone,
    A.email AS asm_email,
    A.page AS fb_ads_page,
    A.pic AS fb_ads_pic
  FROM
    {{ ref("stg_gsheet__asms") }} A
{% endsnapshot %}
