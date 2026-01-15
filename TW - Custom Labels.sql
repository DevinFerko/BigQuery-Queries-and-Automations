SELECT
  offer_id,
  price.value AS price_value,
  product_data_timestamp,
  product_type,
  sale_price_effective_start_date,
  sale_price_effective_end_date,
  sale_price.value AS sale_price_value,
  title
FROM `tech-analytics-data.google_merchant_center_TapWarehouse.Products_8600333`