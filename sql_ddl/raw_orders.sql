CREATE TABLE raw_orders (
    row_id int,
    order_id string,
    order_date string,
    ship_date string,
    ship_mode string,
    customer_id string,
    product_id string,
    quantity int,
    price decimal(10, 2),
    discount decimal(5, 2),
    profit decimal(10, 2),
    created_dt timestamp,
    created_by string,
    updated_dt timestamp,
    updated_by string
)
USING DELTA;