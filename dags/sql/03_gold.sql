CREATE OR REPLACE TABLE `imposing-bee-510900-u7.gold.sales_by_outlet`
CLUSTER BY outlet_type AS
SELECT
  outlet_identifier, outlet_type, outlet_size, outlet_location_type,
  outlet_establishment_year,
  COUNT(*)                          AS item_count,
  ROUND(SUM(item_outlet_sales), 2)  AS total_sales,
  ROUND(AVG(item_outlet_sales), 2)  AS avg_sales_per_item
FROM `imposing-bee-510900-u7.silver.bigmart_sales`
GROUP BY 1, 2, 3, 4, 5;

CREATE OR REPLACE TABLE `imposing-bee-510900-u7.gold.sales_by_item_type` AS
SELECT
  item_type, item_fat_content,
  COUNT(*)                          AS item_count,
  ROUND(SUM(item_outlet_sales), 2)  AS total_sales,
  ROUND(AVG(item_mrp), 2)           AS avg_mrp
FROM `imposing-bee-510900-u7.silver.bigmart_sales`
GROUP BY 1, 2;

CREATE OR REPLACE TABLE `imposing-bee-510900-u7.gold.sales_by_outlet_type_tier` AS
SELECT
  outlet_type, outlet_location_type,
  COUNT(DISTINCT outlet_identifier) AS outlets,
  ROUND(SUM(item_outlet_sales), 2)  AS total_sales,
  ROUND(SUM(item_outlet_sales) / COUNT(DISTINCT outlet_identifier), 2) AS sales_per_outlet
FROM `imposing-bee-510900-u7.silver.bigmart_sales`
GROUP BY 1, 2;
