CREATE OR REPLACE TABLE `imposing-bee-510900-u7.silver.bigmart_sales`
PARTITION BY RANGE_BUCKET(outlet_establishment_year, GENERATE_ARRAY(1985, 2011, 1))
CLUSTER BY outlet_type, outlet_identifier, item_type
AS
WITH dedup AS (
  SELECT DISTINCT *
  FROM `imposing-bee-510900-u7.vamshi_123.my_csv_data`
  WHERE Item_Identifier IS NOT NULL
    AND Outlet_Identifier IS NOT NULL
    AND Item_Outlet_Sales IS NOT NULL
    AND Item_Outlet_Sales >= 0
)
SELECT
  Item_Identifier                                    AS item_identifier,
  COALESCE(Item_Weight,
           AVG(Item_Weight) OVER (PARTITION BY Item_Identifier)) AS item_weight,
  CASE LOWER(TRIM(Item_Fat_Content))
    WHEN 'lf'      THEN 'Low Fat'
    WHEN 'low fat' THEN 'Low Fat'
    WHEN 'reg'     THEN 'Regular'
    WHEN 'regular' THEN 'Regular'
    ELSE 'Unknown'
  END                                                AS item_fat_content,
  NULLIF(Item_Visibility, 0)                         AS item_visibility,
  Item_Type                                          AS item_type,
  Item_MRP                                           AS item_mrp,
  Outlet_Identifier                                  AS outlet_identifier,
  Outlet_Establishment_Year                          AS outlet_establishment_year,
  COALESCE(NULLIF(TRIM(Outlet_Size), ''), 'Unknown') AS outlet_size,
  Outlet_Location_Type                               AS outlet_location_type,
  Outlet_Type                                        AS outlet_type,
  Item_Outlet_Sales                                  AS item_outlet_sales,
  CURRENT_TIMESTAMP()                                AS processed_at
FROM dedup;
