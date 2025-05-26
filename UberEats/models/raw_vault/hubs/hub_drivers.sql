WITH source AS (
  SELECT * FROM {{ source('uber_eats', 'v_stg_drivers') }}
),

deduplicated AS (
  SELECT DISTINCT
    {{ dbt_utils.generate_surrogate_key(['license_number']) }} AS hash_hub_license_number,
    license_number,
    'trn-driver-postgres' AS bkcc,
    'tenant-br' AS multi_tenant_id,
    load_dts,
    rec_src AS record_source
  FROM source
)

SELECT * FROM deduplicated
