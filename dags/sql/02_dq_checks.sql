ASSERT (SELECT COUNT(*) FROM `imposing-bee-510900-u7.silver.bigmart_sales`)
     = (SELECT COUNT(*) FROM `imposing-bee-510900-u7.vamshi_123.my_csv_data`)
  AS 'Row count mismatch between source and silver';

ASSERT (
  SELECT COUNT(*) FROM (
    SELECT item_identifier, outlet_identifier
    FROM `imposing-bee-510900-u7.silver.bigmart_sales`
    GROUP BY 1, 2 HAVING COUNT(*) > 1)
) = 0 AS 'Duplicate item + outlet rows found';

ASSERT (SELECT COUNTIF(item_outlet_sales IS NULL OR item_outlet_sales < 0)
        FROM `imposing-bee-510900-u7.silver.bigmart_sales`) = 0
  AS 'Invalid sales values';

ASSERT (SELECT COUNTIF(item_fat_content NOT IN ('Low Fat','Regular'))
        FROM `imposing-bee-510900-u7.silver.bigmart_sales`) = 0
  AS 'Unstandardised fat content';
