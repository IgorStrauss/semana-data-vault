WITH source AS (
  SELECT * FROM {{ source('uber_eats', 'v_stg_orders') }}
),

hashed AS (
  SELECT
    {{ dbt_utils.generate_surrogate_key([
      'order_id',
      'cpf',
      'cnpj',
      'license_number'
    ]) }} AS hash_link_order_user_restaurant_driver,

    {{ dbt_utils.generate_surrogate_key(['order_id']) }} AS hash_hub_order_id,
    {{ dbt_utils.generate_surrogate_key(['cpf']) }} AS hash_hub_cpf,
    {{ dbt_utils.generate_surrogate_key(['cnpj']) }} AS hash_hub_cnpj,
    {{ dbt_utils.generate_surrogate_key(['license_number']) }} AS hash_hub_license_number,

    'tenant-br' AS multi_tenant_id,
    load_dts,
    rec_src AS record_source
  FROM source
)

SELECT DISTINCT * FROM hashed
