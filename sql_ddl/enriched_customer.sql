CREATE TABLE customers (
    customer_id string,
    customer_name string,
    email string,
    phone string,
    address string,
    segment string,
    country string,
    city string,
    state string,
    postal_code string,
    region string,
    created_dt timestamp,
    created_by string,
    updated_dt timestamp,
    updated_by string
)
USING DELTA;