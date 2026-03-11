-- Demo data quality dataset for AI-assisted checks
-- Safe for re-run in demos: drops and recreates the demo tables.

DROP TABLE IF EXISTS payments;
DROP TABLE IF EXISTS orders;
DROP TABLE IF EXISTS customers;

CREATE TABLE customers (
    customer_id SERIAL PRIMARY KEY,
    full_name TEXT NOT NULL,
    email TEXT,
    phone TEXT,
    country TEXT,
    city TEXT,
    created_at DATE NOT NULL
);

CREATE TABLE orders (
    order_id SERIAL PRIMARY KEY,
    customer_id INT NOT NULL REFERENCES customers(customer_id),
    order_date DATE NOT NULL,
    shipping_date DATE,
    total_amount NUMERIC(12,2),
    currency CHAR(3) NOT NULL,
    status TEXT NOT NULL
);

CREATE TABLE payments (
    payment_id SERIAL PRIMARY KEY,
    order_id INT NOT NULL REFERENCES orders(order_id),
    payment_date DATE,
    amount NUMERIC(12,2),
    payment_method TEXT,
    payment_status TEXT NOT NULL
);

INSERT INTO customers (full_name, email, phone, country, city, created_at) VALUES
('John Doe', 'john.doe@example.com', '+1-202-555-0100', 'US', 'New York', '2024-01-10'),
('Jon Doe', 'john.doe@example.com', '+1-202-555-0100', 'USA', 'New York', '2024-02-14'),
('Maria Garcia', 'maria.garcia(at)email.com', '+34-600-123-222', 'Spain', 'Madrid', '2024-03-01'),
('Aisha Khan', NULL, '+44-7700-900101', 'UK', 'London', '2024-04-02'),
('Liam Smith', 'liam.smith@example.com', NULL, 'United States', 'Austin', '2024-05-06'),
('Chen Wei', 'chen.wei@example', '+86-138-0013-0000', 'CN', 'Shenzhen', '2024-05-08'),
('Elena Petrova', 'elena.petrova@example.com', '+48-500-600-700', 'Poland', 'Warsaw', '2024-06-09'),
('Elena Petrova ', 'elena.petrovaa@example.com', '+48-500-600-700', 'PL', 'Warszawa', '2024-06-10'),
('Noah Brown', 'noah.brown@example.com', '+1-202-555-0199', '', 'Chicago', '2024-07-11'),
('Priya Nair', 'priya.nair@example.com', '+91-99000-11223', 'India', 'Bengaluru', '2024-08-20');

INSERT INTO orders (customer_id, order_date, shipping_date, total_amount, currency, status) VALUES
(1, '2025-01-05', '2025-01-07', 120.50, 'USD', 'shipped'),
(2, '2025-01-06', '2025-01-05', 120.50, 'USD', 'delivered'),
(3, '2025-01-10', NULL, -25.00, 'EUR', 'pending'),
(4, '2032-03-01', NULL, 89.99, 'GBP', 'pending'),
(5, '2025-02-01', '2025-02-04', NULL, 'USD', 'processing'),
(6, '2025-02-14', '2025-02-16', 0.00, 'USD', 'paid'),
(7, '2025-03-10', '2025-03-12', 250.00, 'PLN', 'cancelled'),
(8, '2025-03-10', '2025-03-15', 250.00, 'PLN', 'shipped'),
(9, '2025-04-02', NULL, 99999.99, 'USD', 'pending'),
(10, '2025-04-05', '2025-04-08', 49.90, 'INR', 'shipped');

INSERT INTO payments (order_id, payment_date, amount, payment_method, payment_status) VALUES
(1, '2025-01-05', 120.50, 'card', 'paid'),
(2, '2025-01-04', 120.50, 'card', 'paid'),
(3, '2025-01-11', -25.00, 'bank_transfer', 'paid'),
(4, NULL, 89.99, 'card', 'pending'),
(5, '2025-02-02', 49.90, 'card', 'paid'),
(6, '2025-02-14', 0.00, 'card', 'paid'),
(7, '2025-03-11', 250.00, 'cash', 'failed'),
(8, '2025-03-11', 500.00, 'card', 'paid'),
(9, '2025-04-03', NULL, 'crypto', 'paid'),
(10, '2025-04-06', 49.90, 'card', 'refunded');
