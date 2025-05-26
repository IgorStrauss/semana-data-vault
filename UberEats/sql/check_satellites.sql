-- 1
SELECT
  hash_hub_license_number,
  load_dts,
  COUNT(*) AS cnt
FROM public_dv_raw.sat_driver_details
GROUP BY 1, 2
HAVING COUNT(*) > 1;

-- 2
SELECT *
FROM public_dv_raw.sat_user_details_mongodb
WHERE hash_hub_cpf IS NULL OR load_dts IS NULL;

-- 3
SELECT *
FROM public_dv_raw.sat_restaurant_details
WHERE hash_diff IS NULL OR record_source IS NULL OR multi_tenant_id IS NULL;

-- 4
SELECT
  COUNT(*) AS total_rows,
  COUNT(DISTINCT hash_diff) AS distinct_hashes,
  COUNT(DISTINCT hash_hub_cnpj) AS distinct_keys
FROM public_dv_raw.sat_restaurant_details;

-- 5
SELECT
  DATE_TRUNC('DAY', load_dts) AS load_day,
  COUNT(*) AS records_loaded
FROM public_dv_raw.sat_order_status
GROUP BY 1
ORDER BY 1 DESC;

-- 6
SELECT
  hash_hub_license_number,
  COUNT(*) AS versions
FROM public_dv_raw.sat_driver_details
GROUP BY hash_hub_license_number
ORDER BY versions DESC
LIMIT 10;

-- 7
SELECT *
FROM public_dv_raw.sat_user_details_mssql
WHERE hash_diff IN (
  SELECT hash_diff
  FROM public_dv_raw.sat_user_details_mssql
  GROUP BY hash_diff
  HAVING COUNT(*) > 1
)
ORDER BY hash_hub_cpf, load_dts;

-- 8
SELECT *
FROM (
  SELECT *,
         ROW_NUMBER() OVER (PARTITION BY hash_hub_order_id ORDER BY load_dts DESC) AS rn
  FROM public_dv_raw.sat_order_details
)
WHERE rn = 1;

-- 9
SELECT 'sat_order_details' AS table_name, COUNT(*) AS rw FROM public_dv_raw.sat_order_details
UNION ALL
SELECT 'sat_user_details_mssql', COUNT(*) FROM public_dv_raw.sat_user_details_mssql
UNION ALL
SELECT 'sat_user_details_mongodb', COUNT(*) FROM public_dv_raw.sat_user_details_mongodb
UNION ALL
SELECT 'sat_restaurant_details', COUNT(*) FROM public_dv_raw.sat_restaurant_details
UNION ALL
SELECT 'sat_driver_details', COUNT(*) FROM public_dv_raw.sat_driver_details;

-- 10
SELECT
  hash_hub_license_number,
  hash_diff,
  MIN(load_dts) AS first_seen,
  MAX(load_dts) AS last_seen,
  COUNT(*) AS version_count
FROM public_dv_raw.sat_driver_details
GROUP BY hash_hub_license_number, hash_diff
ORDER BY hash_hub_license_number, first_seen;

-- 11
WITH staged_drivers AS (
  SELECT
    license_number,
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
    ), 256) AS staged_hash_diff
  FROM public.v_stg_drivers
) , latest_satellite AS (
  SELECT *
  FROM (
    SELECT *,
           ROW_NUMBER() OVER (PARTITION BY hash_hub_license_number ORDER BY load_dts DESC) AS rn
    FROM public_dv_raw.sat_driver_details
  )
  WHERE rn = 1
) , comparison AS (
  SELECT
    SHA2(license_number, 256) AS hash_hub_license_number,
    s.*,
    sat.hash_diff AS latest_hash_diff,
    sat.load_dts AS last_seen,
    CASE
      WHEN sat.hash_diff IS NULL THEN 'NEW DRIVER'
      WHEN s.staged_hash_diff = sat.hash_diff THEN 'NO CHANGE'
      ELSE 'CHANGED'
    END AS change_status
  FROM staged_drivers s
  LEFT JOIN latest_satellite sat
    ON SHA2(s.license_number, 256) = sat.hash_hub_license_number
)
SELECT
  license_number,
  staged_hash_diff,
  latest_hash_diff,
  last_seen,
  change_status
FROM comparison
WHERE change_status <> 'NEW DRIVER'
ORDER BY change_status, license_number;