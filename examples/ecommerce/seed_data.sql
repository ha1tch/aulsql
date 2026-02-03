-- Seed Data for E-Commerce Example
-- Run after ecommerce_schema.sql for a more complete dataset

-- Additional Categories (subcategories)
INSERT INTO categories (name, slug, description, parent_id, sort_order)
VALUES ('Laptops', 'laptops', 'Portable computers', 1, 1)

INSERT INTO categories (name, slug, description, parent_id, sort_order)
VALUES ('Audio', 'audio', 'Headphones, speakers, and more', 1, 2)

INSERT INTO categories (name, slug, description, parent_id, sort_order)
VALUES ('Men', 'men', 'Men''s clothing', 2, 1)

INSERT INTO categories (name, slug, description, parent_id, sort_order)
VALUES ('Women', 'women', 'Women''s clothing', 2, 2)

-- Additional Products
INSERT INTO products (sku, name, slug, description, price, cost, is_featured)
VALUES ('ELEC-003', 'Mechanical Keyboard RGB', 'mechanical-keyboard-rgb',
        'Cherry MX switches, per-key RGB lighting, aluminum frame', 129.99, 52.00, 1)

INSERT INTO products (sku, name, slug, description, price, cost)
VALUES ('ELEC-004', 'Wireless Mouse Ergonomic', 'wireless-mouse-ergonomic',
        'Vertical design, 6 buttons, USB-C rechargeable', 59.99, 22.00)

INSERT INTO products (sku, name, slug, description, price, cost, is_featured)
VALUES ('ELEC-005', '4K Webcam Pro', 'webcam-4k-pro',
        '4K resolution, auto-focus, built-in ring light', 179.99, 68.00, 1)

INSERT INTO products (sku, name, slug, description, price, cost)
VALUES ('CLTH-002', 'Merino Wool Sweater', 'merino-wool-sweater',
        'Lightweight, temperature regulating, multiple colors', 89.99, 35.00)

INSERT INTO products (sku, name, slug, description, price, cost)
VALUES ('CLTH-003', 'Running Shorts Quick-Dry', 'running-shorts-quick-dry',
        'Breathable fabric, zip pocket, reflective details', 34.99, 12.00)

INSERT INTO products (sku, name, slug, description, price, cost)
VALUES ('HOME-002', 'Smart LED Desk Lamp', 'smart-led-desk-lamp',
        'Adjustable color temperature, USB charging port, touch control', 64.99, 24.00)

INSERT INTO products (sku, name, slug, description, price, cost)
VALUES ('HOME-003', 'Bamboo Organizer Set', 'bamboo-organizer-set',
        'Desk organizer with drawer, eco-friendly bamboo', 44.99, 16.00)

-- Link new products to categories
INSERT INTO product_categories (product_id, category_id) VALUES (5, 1)   -- Keyboard -> Electronics
INSERT INTO product_categories (product_id, category_id) VALUES (6, 1)   -- Mouse -> Electronics
INSERT INTO product_categories (product_id, category_id) VALUES (7, 1)   -- Webcam -> Electronics
INSERT INTO product_categories (product_id, category_id) VALUES (8, 2)   -- Sweater -> Clothing
INSERT INTO product_categories (product_id, category_id) VALUES (9, 2)   -- Shorts -> Clothing
INSERT INTO product_categories (product_id, category_id) VALUES (10, 3)  -- Lamp -> Home
INSERT INTO product_categories (product_id, category_id) VALUES (11, 3)  -- Organizer -> Home

-- Inventory for new products
INSERT INTO inventory (product_id, quantity, reorder_level, reorder_quantity)
VALUES (5, 75, 15, 50)

INSERT INTO inventory (product_id, quantity, reorder_level, reorder_quantity)
VALUES (6, 200, 40, 100)

INSERT INTO inventory (product_id, quantity, reorder_level, reorder_quantity)
VALUES (7, 45, 10, 30)

INSERT INTO inventory (product_id, quantity, reorder_level, reorder_quantity)
VALUES (8, 120, 25, 75)

INSERT INTO inventory (product_id, quantity, reorder_level, reorder_quantity)
VALUES (9, 250, 50, 150)

INSERT INTO inventory (product_id, quantity, reorder_level, reorder_quantity)
VALUES (10, 60, 12, 40)

INSERT INTO inventory (product_id, quantity, reorder_level, reorder_quantity)
VALUES (11, 90, 20, 60)

-- Additional Customers
INSERT INTO customers (email, password_hash, first_name, last_name, phone)
VALUES ('mike.wilson@example.com', 'hashed_pw_3', 'Mike', 'Wilson', '+1-555-0103')

INSERT INTO customers (email, password_hash, first_name, last_name, phone)
VALUES ('sarah.jones@example.com', 'hashed_pw_4', 'Sarah', 'Jones', '+1-555-0104')

INSERT INTO customers (email, password_hash, first_name, last_name, phone)
VALUES ('david.brown@example.com', 'hashed_pw_5', 'David', 'Brown', '+1-555-0105')

-- Additional Addresses
INSERT INTO addresses (customer_id, address_type, street_1, city, state, postal_code, country, is_default)
VALUES (3, 'shipping', '789 Pine Road', 'Seattle', 'WA', '98101', 'US', 1)

INSERT INTO addresses (customer_id, address_type, street_1, city, state, postal_code, country, is_default)
VALUES (4, 'shipping', '321 Cedar Lane', 'Denver', 'CO', '80201', 'US', 1)

INSERT INTO addresses (customer_id, address_type, street_1, city, state, postal_code, country, is_default)
VALUES (5, 'shipping', '555 Maple Drive', 'Chicago', 'IL', '60601', 'US', 1)

-- Sample Orders (completed)
INSERT INTO orders (order_number, customer_id, status, subtotal, tax, shipping, total, shipping_address_id)
VALUES ('ORD-20260115-0001', 1, 'delivered', 199.98, 16.00, 0, 215.98, 1)

INSERT INTO order_items (order_id, product_id, quantity, unit_price, total)
VALUES (1, 1, 1, 149.99, 149.99)

INSERT INTO order_items (order_id, product_id, quantity, unit_price, total)
VALUES (1, 2, 1, 49.99, 49.99)

INSERT INTO payments (order_id, payment_method, transaction_id, amount, status, processed_at)
VALUES (1, 'credit_card', 'txn_abc123', 215.98, 'completed', '2026-01-15 14:30:00')

-- Another completed order
INSERT INTO orders (order_number, customer_id, status, subtotal, tax, shipping, total, shipping_address_id)
VALUES ('ORD-20260120-0002', 2, 'delivered', 89.98, 7.20, 9.99, 107.17, 2)

INSERT INTO order_items (order_id, product_id, quantity, unit_price, total)
VALUES (2, 3, 2, 24.99, 49.98)

INSERT INTO order_items (order_id, product_id, quantity, unit_price, total)
VALUES (2, 4, 1, 34.99, 34.99)

INSERT INTO payments (order_id, payment_method, transaction_id, amount, status, processed_at)
VALUES (2, 'paypal', 'pp_xyz789', 107.17, 'completed', '2026-01-20 09:15:00')

-- Pending order
INSERT INTO orders (order_number, customer_id, status, subtotal, tax, shipping, total, shipping_address_id)
VALUES ('ORD-20260130-0003', 3, 'pending', 179.99, 14.40, 0, 194.39, 3)

INSERT INTO order_items (order_id, product_id, quantity, unit_price, total)
VALUES (3, 7, 1, 179.99, 179.99)

-- Update inventory to reflect sold items (deduct from quantity)
UPDATE inventory SET quantity = quantity - 1 WHERE product_id = 1
UPDATE inventory SET quantity = quantity - 1 WHERE product_id = 2
UPDATE inventory SET quantity = quantity - 2 WHERE product_id = 3
UPDATE inventory SET quantity = quantity - 1 WHERE product_id = 4

-- Reserve inventory for pending order
UPDATE inventory SET reserved = 1 WHERE product_id = 7

-- Sample Reviews
INSERT INTO reviews (product_id, customer_id, rating, title, content, is_verified, is_approved)
VALUES (1, 1, 5, 'Amazing sound quality', 'Best headphones I''ve ever owned. Noise cancellation is incredible.', 1, 1)

INSERT INTO reviews (product_id, customer_id, rating, title, content, is_verified, is_approved)
VALUES (1, 2, 4, 'Great but pricey', 'Excellent product, just wish it was a bit cheaper.', 1, 1)

INSERT INTO reviews (product_id, customer_id, rating, title, content, is_verified, is_approved)
VALUES (2, 1, 5, 'Perfect hub', 'Works flawlessly with my MacBook. All ports work simultaneously.', 1, 1)

INSERT INTO reviews (product_id, customer_id, rating, title, content, is_verified, is_approved)
VALUES (3, 2, 4, 'Comfortable and well-made', 'Nice quality cotton, fits true to size.', 1, 1)

-- Shopping cart items (active carts)
INSERT INTO cart_items (customer_id, product_id, quantity)
VALUES (4, 5, 1)

INSERT INTO cart_items (customer_id, product_id, quantity)
VALUES (4, 6, 1)

INSERT INTO cart_items (customer_id, product_id, quantity)
VALUES (5, 8, 2)

-- Final verification
SELECT 'Setup complete!' AS message

SELECT 'products' AS table_name, COUNT(*) AS rows FROM products
UNION ALL SELECT 'categories', COUNT(*) FROM categories
UNION ALL SELECT 'customers', COUNT(*) FROM customers
UNION ALL SELECT 'orders', COUNT(*) FROM orders
UNION ALL SELECT 'reviews', COUNT(*) FROM reviews
UNION ALL SELECT 'cart_items', COUNT(*) FROM cart_items
