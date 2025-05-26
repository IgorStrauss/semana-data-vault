WITH source AS (
  SELECT * FROM {{ source('uber_eats', 'v_stg_status') }}
),

hashed AS (
  SELECT
    {{ dbt_utils.generate_surrogate_key(['order_id']) }} AS hash_hub_order_id,

    status_name,
    status_timestamp,
    status_id,

    SHA2(CONCAT_WS('|',
      status_name, status_timestamp, status_id
    ), 256) AS hash_diff,

    'tenant-br' AS multi_tenant_id,
    load_dts,
    rec_src AS record_source
  FROM source
)

SELECT DISTINCT * FROM hashed
