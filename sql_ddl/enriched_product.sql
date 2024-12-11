CREATE TABLE products (
    product_id string,
    category string,
    subcategory string,
    product_name string,
    state string,
    price_per_product decimal(10, 2),
    created_dt timestamp,
    created_by string,
    updated_dt timestamp,
    updated_by string
)
USING DELTA;