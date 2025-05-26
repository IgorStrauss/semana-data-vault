WITH source AS (
  SELECT * FROM {{ source('uber_eats', 'v_stg_users_mssql') }}
),

hashed AS (
  SELECT
    {{ dbt_utils.generate_surrogate_key(['cpf']) }} AS hash_hub_cpf,

    user_id,
    first_name,
    last_name,
    birthday,
    job,
    phone_number,
    company_name,
    country,

    SHA2(CONCAT_WS('|',
      user_id, first_name, last_name, birthday, job, phone_number, company_name, country
    ), 256) AS hash_diff,

    'tenant-br' AS multi_tenant_id,
    load_dts,
    rec_src AS record_source
  FROM source
)

SELECT DISTINCT * FROM hashed
