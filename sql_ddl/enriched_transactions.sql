CREATE TABLE transactions (
    row_id int,
    order_id string,
    order_date date,
    ship_date date,
    ship_mode string,
    quantity int,
    price decimal(10, 2),
    discount decimal(5, 2),
    profit decimal(10, 2),
    customer_name string,
	country string,
    product_category string,
    product_sub_category string,
    created_dt timestamp,
    created_by string,
    updated_dt timestamp,
    updated_by string
)
USING DELTA;