-- Create DWS Orders Table
CREATE TABLE IF NOT EXISTS dws_orders (
    id BIGINT AUTO_INCREMENT PRIMARY KEY,
    order_id VARCHAR(255) NOT NULL UNIQUE,
    customer_id BIGINT,
    order_amount DECIMAL(10,2),
    order_status VARCHAR(50),
    order_date DATETIME,
    payment_method VARCHAR(50),
    shipping_address TEXT,
    push_status VARCHAR(20) DEFAULT 'pending' COMMENT 'pending, pushed, failed',
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    INDEX idx_order_id (order_id),
    INDEX idx_push_status (push_status),
    INDEX idx_created_at (created_at)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci COMMENT='DWS Orders Table - Data Warehouse Service Layer';