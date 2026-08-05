CREATE TABLE orders (
    id UUID PRIMARY KEY,
    customer_id VARCHAR(150) NOT NULL,
    total NUMERIC(19, 2) NOT NULL
);
