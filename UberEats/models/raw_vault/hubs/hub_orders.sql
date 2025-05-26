WITH source AS (
  SELECT * FROM {{ source('uber_eats', 'v_stg_orders') }}
),

deduplicated AS (
  SELECT DISTINCT
    {{ dbt_utils.generate_surrogate_key(['order_id']) }} AS hash_hub_order_id,
    order_id,
    'trn-order-kafka' AS bkcc,
    'tenant-br' AS multi_tenant_id,
    load_dts,
    rec_src AS record_source
  FROM source
)

SELECT * FROM deduplicated
