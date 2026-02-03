-- E-Commerce Schema (T-SQL)
-- For use with aul/iaul

-- Customers table
CREATE TABLE customers (
    id INT IDENTITY(1,1) PRIMARY KEY,
    email NVARCHAR(100) NOT NULL UNIQUE,
    password_hash NVARCHAR(255) NOT NULL,
    first_name NVARCHAR(50) NOT NULL,
    last_name NVARCHAR(50) NOT NULL,
    phone NVARCHAR(20),
    is_active BIT DEFAULT 1,
    created_at DATETIME DEFAULT GETDATE(),
    updated_at DATETIME DEFAULT GETDATE()
)

-- Customer addresses
CREATE TABLE addresses (
    id INT IDENTITY(1,1) PRIMARY KEY,
    customer_id INT NOT NULL,
    address_type NVARCHAR(20) DEFAULT 'shipping',
    street_1 NVARCHAR(200) NOT NULL,
    street_2 NVARCHAR(200),
    city NVARCHAR(100) NOT NULL,
    state NVARCHAR(50),
    postal_code NVARCHAR(20) NOT NULL,
    country NVARCHAR(50) NOT NULL DEFAULT 'US',
    is_default BIT DEFAULT 0,
    FOREIGN KEY (customer_id) REFERENCES customers(id) ON DELETE CASCADE
)

-- Product categories
CREATE TABLE categories (
    id INT IDENTITY(1,1) PRIMARY KEY,
    name NVARCHAR(100) NOT NULL,
    slug NVARCHAR(100) NOT NULL UNIQUE,
    description NVARCHAR(500),
    parent_id INT NULL,
    sort_order INT DEFAULT 0,
    is_active BIT DEFAULT 1,
    FOREIGN KEY (parent_id) REFERENCES categories(id)
)

-- Products
CREATE TABLE products (
    id INT IDENTITY(1,1) PRIMARY KEY,
    sku NVARCHAR(50) NOT NULL UNIQUE,
    name NVARCHAR(200) NOT NULL,
    slug NVARCHAR(200) NOT NULL UNIQUE,
    description NVARCHAR(MAX),
    price DECIMAL(10,2) NOT NULL,
    cost DECIMAL(10,2),
    weight DECIMAL(8,3),
    is_active BIT DEFAULT 1,
    is_featured BIT DEFAULT 0,
    created_at DATETIME DEFAULT GETDATE(),
    updated_at DATETIME DEFAULT GETDATE()
)

-- Product-Category relationship
CREATE TABLE product_categories (
    product_id INT NOT NULL,
    category_id INT NOT NULL,
    PRIMARY KEY (product_id, category_id),
    FOREIGN KEY (product_id) REFERENCES products(id) ON DELETE CASCADE,
    FOREIGN KEY (category_id) REFERENCES categories(id) ON DELETE CASCADE
)

-- Inventory tracking
CREATE TABLE inventory (
    id INT IDENTITY(1,1) PRIMARY KEY,
    product_id INT NOT NULL UNIQUE,
    quantity INT NOT NULL DEFAULT 0,
    reserved INT NOT NULL DEFAULT 0,
    reorder_level INT DEFAULT 10,
    reorder_quantity INT DEFAULT 50,
    updated_at DATETIME DEFAULT GETDATE(),
    FOREIGN KEY (product_id) REFERENCES products(id) ON DELETE CASCADE
)

-- Orders
CREATE TABLE orders (
    id INT IDENTITY(1,1) PRIMARY KEY,
    order_number NVARCHAR(20) NOT NULL UNIQUE,
    customer_id INT NOT NULL,
    status NVARCHAR(20) DEFAULT 'pending',
    subtotal DECIMAL(10,2) NOT NULL DEFAULT 0,
    tax DECIMAL(10,2) NOT NULL DEFAULT 0,
    shipping DECIMAL(10,2) NOT NULL DEFAULT 0,
    total DECIMAL(10,2) NOT NULL DEFAULT 0,
    shipping_address_id INT,
    billing_address_id INT,
    notes NVARCHAR(500),
    created_at DATETIME DEFAULT GETDATE(),
    updated_at DATETIME DEFAULT GETDATE(),
    FOREIGN KEY (customer_id) REFERENCES customers(id),
    FOREIGN KEY (shipping_address_id) REFERENCES addresses(id),
    FOREIGN KEY (billing_address_id) REFERENCES addresses(id)
)

-- Order line items
CREATE TABLE order_items (
    id INT IDENTITY(1,1) PRIMARY KEY,
    order_id INT NOT NULL,
    product_id INT NOT NULL,
    quantity INT NOT NULL,
    unit_price DECIMAL(10,2) NOT NULL,
    total DECIMAL(10,2) NOT NULL,
    FOREIGN KEY (order_id) REFERENCES orders(id) ON DELETE CASCADE,
    FOREIGN KEY (product_id) REFERENCES products(id)
)

-- Payment transactions
CREATE TABLE payments (
    id INT IDENTITY(1,1) PRIMARY KEY,
    order_id INT NOT NULL,
    payment_method NVARCHAR(50) NOT NULL,
    transaction_id NVARCHAR(100),
    amount DECIMAL(10,2) NOT NULL,
    status NVARCHAR(20) DEFAULT 'pending',
    processed_at DATETIME,
    created_at DATETIME DEFAULT GETDATE(),
    FOREIGN KEY (order_id) REFERENCES orders(id)
)

-- Shopping cart
CREATE TABLE cart_items (
    id INT IDENTITY(1,1) PRIMARY KEY,
    customer_id INT NOT NULL,
    product_id INT NOT NULL,
    quantity INT NOT NULL DEFAULT 1,
    added_at DATETIME DEFAULT GETDATE(),
    FOREIGN KEY (customer_id) REFERENCES customers(id) ON DELETE CASCADE,
    FOREIGN KEY (product_id) REFERENCES products(id) ON DELETE CASCADE,
    UNIQUE (customer_id, product_id)
)

-- Product reviews
CREATE TABLE reviews (
    id INT IDENTITY(1,1) PRIMARY KEY,
    product_id INT NOT NULL,
    customer_id INT NOT NULL,
    rating INT NOT NULL,
    title NVARCHAR(200),
    content NVARCHAR(MAX),
    is_verified BIT DEFAULT 0,
    is_approved BIT DEFAULT 0,
    created_at DATETIME DEFAULT GETDATE(),
    FOREIGN KEY (product_id) REFERENCES products(id) ON DELETE CASCADE,
    FOREIGN KEY (customer_id) REFERENCES customers(id),
    CONSTRAINT chk_rating CHECK (rating >= 1 AND rating <= 5)
)

-- Indexes
CREATE INDEX idx_products_sku ON products(sku)
CREATE INDEX idx_products_active ON products(is_active)
CREATE INDEX idx_orders_customer ON orders(customer_id)
CREATE INDEX idx_orders_status ON orders(status)
CREATE INDEX idx_orders_number ON orders(order_number)
CREATE INDEX idx_inventory_product ON inventory(product_id)
CREATE INDEX idx_cart_customer ON cart_items(customer_id)

-- Sample data: Categories
INSERT INTO categories (name, slug, description, sort_order)
VALUES ('Electronics', 'electronics', 'Gadgets and devices', 1)

INSERT INTO categories (name, slug, description, sort_order)
VALUES ('Clothing', 'clothing', 'Apparel and accessories', 2)

INSERT INTO categories (name, slug, description, sort_order)
VALUES ('Home & Garden', 'home-garden', 'For your living space', 3)

-- Sample data: Products
INSERT INTO products (sku, name, slug, description, price, cost)
VALUES ('ELEC-001', 'Wireless Bluetooth Headphones', 'wireless-bluetooth-headphones',
        'Premium noise-canceling headphones with 30-hour battery life', 149.99, 65.00)

INSERT INTO products (sku, name, slug, description, price, cost)
VALUES ('ELEC-002', 'USB-C Hub 7-in-1', 'usb-c-hub-7in1',
        'Multiport adapter with HDMI, USB-A, SD card reader', 49.99, 18.00)

INSERT INTO products (sku, name, slug, description, price, cost)
VALUES ('CLTH-001', 'Cotton T-Shirt Classic', 'cotton-tshirt-classic',
        '100% organic cotton, available in multiple colors', 24.99, 8.00)

INSERT INTO products (sku, name, slug, description, price, cost)
VALUES ('HOME-001', 'Ceramic Plant Pot Set', 'ceramic-plant-pot-set',
        'Set of 3 minimalist planters with drainage', 34.99, 12.00)

-- Link products to categories
INSERT INTO product_categories (product_id, category_id) VALUES (1, 1)
INSERT INTO product_categories (product_id, category_id) VALUES (2, 1)
INSERT INTO product_categories (product_id, category_id) VALUES (3, 2)
INSERT INTO product_categories (product_id, category_id) VALUES (4, 3)

-- Initialize inventory
INSERT INTO inventory (product_id, quantity, reorder_level, reorder_quantity)
VALUES (1, 150, 20, 100)

INSERT INTO inventory (product_id, quantity, reorder_level, reorder_quantity)
VALUES (2, 300, 50, 200)

INSERT INTO inventory (product_id, quantity, reorder_level, reorder_quantity)
VALUES (3, 500, 100, 300)

INSERT INTO inventory (product_id, quantity, reorder_level, reorder_quantity)
VALUES (4, 80, 15, 50)

-- Sample data: Customers
INSERT INTO customers (email, password_hash, first_name, last_name, phone)
VALUES ('john.doe@example.com', 'hashed_pw_1', 'John', 'Doe', '+1-555-0101')

INSERT INTO customers (email, password_hash, first_name, last_name, phone)
VALUES ('jane.smith@example.com', 'hashed_pw_2', 'Jane', 'Smith', '+1-555-0102')

-- Sample addresses
INSERT INTO addresses (customer_id, address_type, street_1, city, state, postal_code, country, is_default)
VALUES (1, 'shipping', '123 Main Street', 'Austin', 'TX', '78701', 'US', 1)

INSERT INTO addresses (customer_id, address_type, street_1, city, state, postal_code, country, is_default)
VALUES (2, 'shipping', '456 Oak Avenue', 'Portland', 'OR', '97201', 'US', 1)

-- Verify setup
SELECT 'products' AS table_name, COUNT(*) AS row_count FROM products
UNION ALL
SELECT 'categories', COUNT(*) FROM categories
UNION ALL
SELECT 'inventory', COUNT(*) FROM inventory
UNION ALL
SELECT 'customers', COUNT(*) FROM customers
