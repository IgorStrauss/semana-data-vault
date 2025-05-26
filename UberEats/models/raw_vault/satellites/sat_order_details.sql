WITH source AS (
  SELECT * FROM {{ source('uber_eats', 'v_stg_orders') }}
),

hashed AS (
  SELECT
    {{ dbt_utils.generate_surrogate_key(['order_id']) }} AS hash_hub_order_id,

    order_date,
    total_amount,
    payment_key,

    SHA2(CONCAT_WS('|',
      order_date, total_amount, payment_key
    ), 256) AS hash_diff,

    'tenant-br' AS multi_tenant_id,
    load_dts,
    rec_src AS record_source
  FROM source
)

SELECT DISTINCT * FROM hashed
