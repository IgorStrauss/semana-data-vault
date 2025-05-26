-- 1
SELECT COUNT(*) AS row_count FROM public_dv_raw.hub_orders;
SELECT COUNT(*) AS row_count FROM public_dv_raw.hub_users;
SELECT COUNT(*) AS row_count FROM public_dv_raw.hub_restaurants;
SELECT COUNT(*) AS row_count FROM public_dv_raw.hub_drivers;

-- 2
SELECT order_id, COUNT(*) AS cnt
FROM public_dv_raw.hub_orders
GROUP BY order_id
HAVING COUNT(*) > 1;

SELECT cpf, COUNT(*) AS cnt
FROM public_dv_raw.hub_users
GROUP BY cpf
HAVING COUNT(*) > 1;

-- 3
SELECT * FROM public_dv_raw.hub_orders WHERE order_id IS NULL;
SELECT * FROM public_dv_raw.hub_users WHERE cpf IS NULL;

-- 4
SELECT hash_hub_order_id, order_id, bkcc, multi_tenant_id
FROM public_dv_raw.hub_orders
LIMIT 5;

-- 5
SELECT DISTINCT bkcc FROM public_dv_raw.hub_orders;
SELECT DISTINCT bkcc FROM public_dv_raw.hub_users;

-- 6
SELECT
  DATE_TRUNC('DAY', load_dts) AS load_day,
  COUNT(*) AS rows_loaded
FROM public_dv_raw.hub_orders
GROUP BY 1
ORDER BY 1 DESC;

-- 7
SELECT record_source, COUNT(*) AS dt
FROM public_dv_raw.hub_users
GROUP BY record_source;
