WITH source AS (
  SELECT * FROM {{ source('uber_eats', 'v_stg_drivers') }}
),

hashed AS (
  SELECT
    {{ dbt_utils.generate_surrogate_key(['license_number']) }} AS hash_hub_license_number,

    driver_id,
    first_name,
    last_name,
    date_birth,
    phone_number,
    city,
    country,
    vehicle_type,
    vehicle_make,
    vehicle_model,
    vehicle_year,

    SHA2(CONCAT_WS('|',
      driver_id, first_name, last_name, date_birth, phone_number,
      city, country, vehicle_type, vehicle_make, vehicle_model, vehicle_year
    ), 256) AS hash_diff,

    'tenant-br' AS multi_tenant_id,
    load_dts,
    rec_src AS record_source
  FROM source
)

SELECT DISTINCT * FROM hashed
