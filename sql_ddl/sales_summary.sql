CREATE TABLE sales_summary (
    year INT,
    product_category string,
    product_subcategory string,
    customer_name string,
    total_profit decimal(10, 2),
    created_dt timestamp,
    created_by string,
    updated_dt timestamp,
    updated_by string
)
USING DELTA;