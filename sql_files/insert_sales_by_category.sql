TRUNCATE TABLE sales_by_category;

INSERT INTO sales_by_category (PRODUCT_CATEGORY, total_sales, avg_margin, record_count)
SELECT
    PRODUCT_CATEGORY,
    ROUND(SUM(SP), 2) AS total_sales,
    ROUND(AVG(SP - CP), 2) AS avg_margin,
    COUNT(*) AS record_count
FROM store_transactions
GROUP BY PRODUCT_CATEGORY
ORDER BY total_sales DESC;
