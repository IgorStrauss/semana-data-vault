-- 1
SELECT COUNT(*) FROM public_dv_raw.link_order_user_restaurant_driver;

-- 2
SELECT
  hash_link_order_user_restaurant_driver,
  COUNT(*) AS cnt
FROM public_dv_raw.link_order_user_restaurant_driver
GROUP BY 1
HAVING COUNT(*) > 1;

-- 3
SELECT
  COUNT(*) AS total_links,
  COUNT(DISTINCT hash_hub_order_id) AS unique_orders,
  COUNT(DISTINCT hash_hub_cpf) AS unique_users,
  COUNT(DISTINCT hash_hub_cnpj) AS unique_restaurants,
  COUNT(DISTINCT hash_hub_license_number) AS unique_drivers
FROM public_dv_raw.link_order_user_restaurant_driver;

-- 4
SELECT l.hash_hub_order_id, o.order_id
FROM public_dv_raw.link_order_user_restaurant_driver l
JOIN public_dv_raw.hub_orders o
  ON l.hash_hub_order_id = o.hash_hub_order_id
LIMIT 10;
