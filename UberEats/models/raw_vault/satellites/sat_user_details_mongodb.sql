WITH source AS (
  SELECT * FROM {{ source('uber_eats', 'v_stg_users_mongodb') }}
),

hashed AS (
  SELECT
    {{ dbt_utils.generate_surrogate_key(['cpf']) }} AS hash_hub_cpf,

    user_id,
    city,
    email,
    delivery_address,
    phone_number,
    country,

    SHA2(CONCAT_WS('|',
      user_id, city, email, delivery_address, phone_number, country
    ), 256) AS hash_diff,

    'tenant-br' AS multi_tenant_id,
    load_dts,
    rec_src AS record_source
  FROM source
)

SELECT DISTINCT * FROM hashed
