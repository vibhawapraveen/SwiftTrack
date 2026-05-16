-- SwiftLogistics Database Schema
-- Middleware Architecture Assignment

-- Clients table (for CMS)
CREATE TABLE IF NOT EXISTS clients (
    id SERIAL PRIMARY KEY,
    client_id VARCHAR(50) UNIQUE NOT NULL,
    company_name VARCHAR(255) NOT NULL,
    email VARCHAR(255) UNIQUE NOT NULL,
    password_hash VARCHAR(255) NOT NULL,
    contract_type VARCHAR(50) DEFAULT 'standard',
    billing_address TEXT,
    contact_phone VARCHAR(20),
    status VARCHAR(20) DEFAULT 'active',
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Drivers table
CREATE TABLE IF NOT EXISTS drivers (
    id SERIAL PRIMARY KEY,
    driver_id VARCHAR(50) UNIQUE NOT NULL,
    name VARCHAR(255) NOT NULL,
    email VARCHAR(255) UNIQUE NOT NULL,
    password_hash VARCHAR(255) NOT NULL,
    phone VARCHAR(20),
    vehicle_type VARCHAR(50),
    vehicle_number VARCHAR(50),
    status VARCHAR(20) DEFAULT 'available',
    current_location_lat DECIMAL(10, 8),
    current_location_lng DECIMAL(11, 8),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Orders table
CREATE TABLE IF NOT EXISTS orders (
    id SERIAL PRIMARY KEY,
    order_id VARCHAR(50) UNIQUE NOT NULL,
    client_id VARCHAR(50) REFERENCES clients(client_id),
    pickup_address TEXT NOT NULL,
    delivery_address TEXT NOT NULL,
    pickup_lat DECIMAL(10, 8),
    pickup_lng DECIMAL(11, 8),
    delivery_lat DECIMAL(10, 8),
    delivery_lng DECIMAL(11, 8),
    package_weight DECIMAL(10, 2),
    package_dimensions VARCHAR(100),
    priority VARCHAR(20) DEFAULT 'normal',
    status VARCHAR(50) DEFAULT 'pending',
    estimated_delivery TIMESTAMP,
    actual_delivery TIMESTAMP,
    driver_id VARCHAR(50) REFERENCES drivers(driver_id),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Packages table (for WMS)
CREATE TABLE IF NOT EXISTS packages (
    id SERIAL PRIMARY KEY,
    package_id VARCHAR(50) UNIQUE NOT NULL,
    order_id VARCHAR(50) REFERENCES orders(order_id),
    barcode VARCHAR(100),
    warehouse_location VARCHAR(50),
    status VARCHAR(50) DEFAULT 'received',
    received_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    processed_at TIMESTAMP,
    loaded_at TIMESTAMP,
    delivered_at TIMESTAMP
);

-- Routes table (for ROS)
CREATE TABLE IF NOT EXISTS routes (
    id SERIAL PRIMARY KEY,
    route_id VARCHAR(50) UNIQUE NOT NULL,
    driver_id VARCHAR(50) REFERENCES drivers(driver_id),
    date DATE NOT NULL,
    status VARCHAR(20) DEFAULT 'planned',
    total_distance DECIMAL(10, 2),
    estimated_duration INTEGER,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Route stops table
CREATE TABLE IF NOT EXISTS route_stops (
    id SERIAL PRIMARY KEY,
    route_id VARCHAR(50) REFERENCES routes(route_id),
    order_id VARCHAR(50) REFERENCES orders(order_id),
    sequence_number INTEGER NOT NULL,
    estimated_arrival TIMESTAMP,
    actual_arrival TIMESTAMP,
    status VARCHAR(20) DEFAULT 'pending'
);

-- Delivery proofs table
CREATE TABLE IF NOT EXISTS delivery_proofs (
    id SERIAL PRIMARY KEY,
    order_id VARCHAR(50) REFERENCES orders(order_id),
    proof_type VARCHAR(20) NOT NULL,
    signature_data TEXT,
    photo_url TEXT,
    recipient_name VARCHAR(255),
    notes TEXT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Transaction logs (for distributed transaction management)
CREATE TABLE IF NOT EXISTS transaction_logs (
    id SERIAL PRIMARY KEY,
    transaction_id VARCHAR(50) UNIQUE NOT NULL,
    order_id VARCHAR(50),
    cms_status VARCHAR(20) DEFAULT 'pending',
    ros_status VARCHAR(20) DEFAULT 'pending',
    wms_status VARCHAR(20) DEFAULT 'pending',
    overall_status VARCHAR(20) DEFAULT 'pending',
    retry_count INTEGER DEFAULT 0,
    error_message TEXT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Notifications table
CREATE TABLE IF NOT EXISTS notifications (
    id SERIAL PRIMARY KEY,
    notification_id VARCHAR(50) UNIQUE NOT NULL,
    user_type VARCHAR(20) NOT NULL,
    user_id VARCHAR(50) NOT NULL,
    title VARCHAR(255) NOT NULL,
    message TEXT NOT NULL,
    type VARCHAR(50) DEFAULT 'info',
    is_read BOOLEAN DEFAULT FALSE,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Insert sample data
INSERT INTO clients (client_id, company_name, email, password_hash, contract_type, billing_address, contact_phone)
VALUES 
    ('CLT001', 'TechMart Online', 'techmart@example.com', '$2b$10$rO.X3Xt3KXSO6xF5QGSjK.VHZV3b7X2yVf6mG3q6v3FqYPvKX6.JS', 'premium', '123 Tech Street, Colombo', '+94771234567'),
    ('CLT002', 'Fashion Hub', 'fashionhub@example.com', '$2b$10$rO.X3Xt3KXSO6xF5QGSjK.VHZV3b7X2yVf6mG3q6v3FqYPvKX6.JS', 'standard', '456 Style Avenue, Kandy', '+94772345678'),
    ('CLT003', 'HomeGoods Lanka', 'homegoods@example.com', '$2b$10$rO.X3Xt3KXSO6xF5QGSjK.VHZV3b7X2yVf6mG3q6v3FqYPvKX6.JS', 'enterprise', '789 Home Lane, Galle', '+94773456789')
ON CONFLICT (client_id) DO NOTHING;

INSERT INTO drivers (driver_id, name, email, password_hash, phone, vehicle_type, vehicle_number, status)
VALUES 
    ('DRV001', 'Kasun Perera', 'kasun@swiftlogistics.lk', '$2b$10$rO.X3Xt3KXSO6xF5QGSjK.VHZV3b7X2yVf6mG3q6v3FqYPvKX6.JS', '+94774567890', 'Van', 'WP-KA-1234', 'available'),
    ('DRV002', 'Nimal Silva', 'nimal@swiftlogistics.lk', '$2b$10$rO.X3Xt3KXSO6xF5QGSjK.VHZV3b7X2yVf6mG3q6v3FqYPvKX6.JS', '+94775678901', 'Motorcycle', 'WP-NB-5678', 'available'),
    ('DRV003', 'Samantha Fernando', 'samantha@swiftlogistics.lk', '$2b$10$rO.X3Xt3KXSO6xF5QGSjK.VHZV3b7X2yVf6mG3q6v3FqYPvKX6.JS', '+94776789012', 'Truck', 'WP-SC-9012', 'on_delivery')
ON CONFLICT (driver_id) DO NOTHING;

-- Note: Default password for all users is 'password123'
