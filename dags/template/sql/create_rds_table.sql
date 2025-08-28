CREATE TABLE IF NOT EXISTS customers (
    customer_id INT PRIMARY KEY,
    first_name VARCHAR(50),
    last_name VARCHAR(50),
    address VARCHAR(255),
    email VARCHAR(255)
);

CREATE TABLE IF NOT EXISTS items (
    item_id INT PRIMARY KEY,
    item_name VARCHAR(50),
    unit_price DECIMAL(8, 2)
);

CREATE TABLE IF NOT EXISTS datetable (
    date_id INT PRIMARY KEY,
    date DATETIME,
    year INT,
    month INT,
    day INT,
    time TIME
);

CREATE TABLE IF NOT EXISTS orders (
    order_id INT PRIMARY KEY,
    date_id INT,
    customer_id INT,
    item_id INT,
    quantity INT,
    amount DECIMAL(10, 2)
);