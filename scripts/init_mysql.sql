-- MySQL initialization script for CDC target database
-- This script sets up the destination database structure

-- Use the CDC target database
USE cdc_target;

-- Create target tables that mirror the PostgreSQL source structure
CREATE TABLE IF NOT EXISTS users (
    id VARCHAR(36) PRIMARY KEY,
    username VARCHAR(50) UNIQUE NOT NULL,
    email VARCHAR(100) UNIQUE NOT NULL,
    full_name VARCHAR(100),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    is_active BOOLEAN DEFAULT TRUE,
    
    -- CDC metadata columns
    _cdc_operation VARCHAR(10) DEFAULT 'INSERT',
    _cdc_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    _cdc_source_lsn VARCHAR(50),
    
    INDEX idx_users_username (username),
    INDEX idx_users_email (email),
    INDEX idx_cdc_timestamp (_cdc_timestamp)
);

CREATE TABLE IF NOT EXISTS orders (
    id VARCHAR(36) PRIMARY KEY,
    user_id VARCHAR(36),
    order_number VARCHAR(20) UNIQUE NOT NULL,
    total_amount DECIMAL(10, 2) NOT NULL,
    status VARCHAR(20) DEFAULT 'pending',
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    
    -- CDC metadata columns
    _cdc_operation VARCHAR(10) DEFAULT 'INSERT',
    _cdc_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    _cdc_source_lsn VARCHAR(50),
    
    INDEX idx_orders_user_id (user_id),
    INDEX idx_orders_status (status),
    INDEX idx_orders_order_number (order_number),
    INDEX idx_cdc_timestamp (_cdc_timestamp),
    
    FOREIGN KEY (user_id) REFERENCES users(id) ON DELETE CASCADE
);

CREATE TABLE IF NOT EXISTS order_items (
    id VARCHAR(36) PRIMARY KEY,
    order_id VARCHAR(36),
    product_name VARCHAR(100) NOT NULL,
    quantity INTEGER NOT NULL,
    unit_price DECIMAL(10, 2) NOT NULL,
    subtotal DECIMAL(10, 2) NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    
    -- CDC metadata columns
    _cdc_operation VARCHAR(10) DEFAULT 'INSERT',
    _cdc_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    _cdc_source_lsn VARCHAR(50),
    
    INDEX idx_order_items_order_id (order_id),
    INDEX idx_cdc_timestamp (_cdc_timestamp),
    
    FOREIGN KEY (order_id) REFERENCES orders(id) ON DELETE CASCADE
);

-- Create CDC metadata table to track replication status
CREATE TABLE IF NOT EXISTS _cdc_metadata (
    id INT AUTO_INCREMENT PRIMARY KEY,
    table_name VARCHAR(100) NOT NULL,
    last_lsn VARCHAR(50),
    last_sync_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    record_count BIGINT DEFAULT 0,
    error_count BIGINT DEFAULT 0,
    status ENUM('active', 'paused', 'error') DEFAULT 'active',
    
    UNIQUE KEY uk_table_name (table_name),
    INDEX idx_last_sync_time (last_sync_time)
);

-- Initialize metadata for tracked tables
INSERT INTO _cdc_metadata (table_name, record_count) VALUES 
    ('users', 0),
    ('orders', 0),
    ('order_items', 0)
ON DUPLICATE KEY UPDATE 
    last_sync_time = CURRENT_TIMESTAMP;

-- Create CDC statistics view
CREATE OR REPLACE VIEW cdc_statistics AS
SELECT 
    table_name,
    record_count,
    error_count,
    last_sync_time,
    status,
    TIMESTAMPDIFF(MINUTE, last_sync_time, NOW()) as minutes_since_last_sync
FROM _cdc_metadata
ORDER BY table_name;

-- Create procedure to update CDC metadata
DELIMITER //

CREATE PROCEDURE UpdateCDCMetadata(
    IN p_table_name VARCHAR(100),
    IN p_lsn VARCHAR(50),
    IN p_record_count BIGINT,
    IN p_error_count BIGINT
)
BEGIN
    INSERT INTO _cdc_metadata (table_name, last_lsn, record_count, error_count, last_sync_time)
    VALUES (p_table_name, p_lsn, p_record_count, p_error_count, NOW())
    ON DUPLICATE KEY UPDATE
        last_lsn = p_lsn,
        record_count = record_count + p_record_count,
        error_count = error_count + p_error_count,
        last_sync_time = NOW();
END //

DELIMITER ;

-- Grant necessary privileges to CDC user
GRANT SELECT, INSERT, UPDATE, DELETE ON cdc_target.* TO 'cdc_user'@'%';
GRANT EXECUTE ON PROCEDURE UpdateCDCMetadata TO 'cdc_user'@'%';

-- Display setup information
SELECT 'MySQL CDC target database setup completed!' as status;
SELECT 'Tables created: users, orders, order_items' as tables;
SELECT 'CDC metadata table and procedures created' as metadata;

-- Show table structure
SHOW TABLES;
DESCRIBE users;
DESCRIBE _cdc_metadata;
