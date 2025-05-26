WITH mssql AS (
  SELECT
    {{ dbt_utils.generate_surrogate_key(['cpf']) }} AS hash_hub_cpf,
    cpf,
    'trn-user' AS bkcc,
    'tenant-br' AS multi_tenant_id,
    load_dts,
    rec_src AS record_source
  FROM {{ source('uber_eats', 'v_stg_users_mssql') }}
),
mongodb AS (
  SELECT
    {{ dbt_utils.generate_surrogate_key(['cpf']) }} AS hash_hub_cpf,
    cpf,
    'trn-user' AS bkcc,
    'tenant-br' AS multi_tenant_id,
    load_dts,
    rec_src AS record_source
  FROM {{ source('uber_eats', 'v_stg_users_mongodb') }}
)

SELECT * FROM mssql
UNION
SELECT * FROM mongodb
