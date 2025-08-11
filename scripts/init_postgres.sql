-- PostgreSQL initialization script for CDC setup
-- This script sets up the source database with sample data and CDC configuration

-- Enable the uuid-ossp extension for UUID generation
CREATE EXTENSION IF NOT EXISTS "uuid-ossp";

-- Create sample tables for CDC demonstration
CREATE TABLE IF NOT EXISTS users (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    username VARCHAR(50) UNIQUE NOT NULL,
    email VARCHAR(100) UNIQUE NOT NULL,
    full_name VARCHAR(100),
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    is_active BOOLEAN DEFAULT TRUE
);

CREATE TABLE IF NOT EXISTS orders (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    user_id UUID REFERENCES users(id),
    order_number VARCHAR(20) UNIQUE NOT NULL,
    total_amount DECIMAL(10, 2) NOT NULL,
    status VARCHAR(20) DEFAULT 'pending',
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS order_items (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    order_id UUID REFERENCES orders(id),
    product_name VARCHAR(100) NOT NULL,
    quantity INTEGER NOT NULL,
    unit_price DECIMAL(10, 2) NOT NULL,
    subtotal DECIMAL(10, 2) NOT NULL,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);

-- Create indexes for better performance
CREATE INDEX IF NOT EXISTS idx_users_username ON users(username);
CREATE INDEX IF NOT EXISTS idx_users_email ON users(email);
CREATE INDEX IF NOT EXISTS idx_orders_user_id ON orders(user_id);
CREATE INDEX IF NOT EXISTS idx_orders_status ON orders(status);
CREATE INDEX IF NOT EXISTS idx_order_items_order_id ON order_items(order_id);

-- Insert sample data
INSERT INTO users (username, email, full_name) VALUES
    ('john_doe', 'john.doe@example.com', 'John Doe'),
    ('jane_smith', 'jane.smith@example.com', 'Jane Smith'),
    ('bob_wilson', 'bob.wilson@example.com', 'Bob Wilson')
ON CONFLICT (username) DO NOTHING;

-- Get user IDs for sample orders
DO $$
DECLARE
    john_id UUID;
    jane_id UUID;
    bob_id UUID;
BEGIN
    SELECT id INTO john_id FROM users WHERE username = 'john_doe';
    SELECT id INTO jane_id FROM users WHERE username = 'jane_smith';
    SELECT id INTO bob_id FROM users WHERE username = 'bob_wilson';
    
    -- Insert sample orders
    INSERT INTO orders (user_id, order_number, total_amount, status) VALUES
        (john_id, 'ORD-001', 299.99, 'completed'),
        (jane_id, 'ORD-002', 149.50, 'pending'),
        (bob_id, 'ORD-003', 89.99, 'shipped')
    ON CONFLICT (order_number) DO NOTHING;
END $$;

-- Create publication for logical replication (CDC)
-- This publication will track changes to all tables
SELECT pg_drop_replication_slot('cdc_slot') WHERE EXISTS (
    SELECT 1 FROM pg_replication_slots WHERE slot_name = 'cdc_slot'
);

DROP PUBLICATION IF EXISTS cdc_pub;
CREATE PUBLICATION cdc_pub FOR ALL TABLES;

-- Create replication slot for CDC
SELECT pg_create_logical_replication_slot('cdc_slot', 'pgoutput');

-- Grant necessary permissions for replication
GRANT USAGE ON SCHEMA public TO postgres;
GRANT SELECT ON ALL TABLES IN SCHEMA public TO postgres;
ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT SELECT ON TABLES TO postgres;

-- Create a function to update the updated_at timestamp
CREATE OR REPLACE FUNCTION update_updated_at_column()
RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at = NOW();
    RETURN NEW;
END;
$$ language 'plpgsql';

-- Create triggers to automatically update updated_at columns
CREATE TRIGGER update_users_updated_at BEFORE UPDATE ON users
    FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();

CREATE TRIGGER update_orders_updated_at BEFORE UPDATE ON orders
    FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();

-- Display setup information
\echo 'PostgreSQL CDC setup completed!'
\echo 'Publication: cdc_pub'
\echo 'Replication Slot: cdc_slot'
\echo 'Sample data inserted into users, orders, and order_items tables'

-- Show current publications and replication slots
SELECT pub.pubname, pub.puballtables, pub.pubinsert, pub.pubupdate, pub.pubdelete, pub.pubtruncate
FROM pg_publication pub;

SELECT slot_name, plugin, slot_type, database, active
FROM pg_replication_slots;
