TRUNCATE TABLE sales_by_store;

INSERT INTO sales_by_store (STORE_LOCATION, total_sales, record_count)
SELECT
    STORE_LOCATION,
    ROUND(SUM(SP), 2) AS total_sales,
    COUNT(*) AS record_count
FROM store_transactions
GROUP BY STORE_LOCATION
ORDER BY total_sales DESC;
