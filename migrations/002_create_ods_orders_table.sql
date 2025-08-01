-- 创建ODS层订单表
CREATE TABLE IF NOT EXISTS `ods_orders` (
    `id` bigint(20) NOT NULL AUTO_INCREMENT COMMENT '自增ID',
    `order_id` bigint(20) NOT NULL COMMENT '订单ID',
    `detail` json NOT NULL COMMENT '订单详情JSON',
    `created_at` datetime NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间',
    `updated_at` datetime NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT '更新时间',
    PRIMARY KEY (`id`),
    UNIQUE KEY `uk_order_id` (`order_id`),
    KEY `idx_created_at` (`created_at`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='ODS层订单表';