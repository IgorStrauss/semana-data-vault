WITH source AS (
  SELECT * FROM {{ source('uber_eats', 'v_stg_restaurants') }}
),

hashed AS (
  SELECT
    {{ dbt_utils.generate_surrogate_key(['cnpj']) }} AS hash_hub_cnpj,

    restaurant_id,
    name,
    address,
    city,
    phone_number,
    country,
    cuisine_type,
    opening_time,
    closing_time,
    average_rating,
    num_reviews,

    SHA2(CONCAT_WS('|',
      restaurant_id, name, address, city, phone_number, country,
      cuisine_type, opening_time, closing_time, average_rating, num_reviews
    ), 256) AS hash_diff,

    'tenant-br' AS multi_tenant_id,
    load_dts,
    rec_src AS record_source
  FROM source
)

SELECT DISTINCT * FROM hashed
