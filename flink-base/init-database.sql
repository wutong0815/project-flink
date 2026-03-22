-- ============================================
-- 业务数据处理工作流 - 数据库初始化脚本
-- ============================================

-- 创建数据库
CREATE DATABASE IF NOT EXISTS flink_db 
    DEFAULT CHARACTER SET utf8mb4 
    DEFAULT COLLATE utf8mb4_unicode_ci;

USE flink_db;

-- ============================================
-- 1. 原始订单表 (数据源)
-- ============================================
DROP TABLE IF EXISTS orders;

CREATE TABLE orders (
    order_id VARCHAR(64) PRIMARY KEY COMMENT '订单 ID',
    user_id VARCHAR(64) NOT NULL COMMENT '用户 ID',
    amount DECIMAL(10,2) NOT NULL COMMENT '订单金额',
    product VARCHAR(255) COMMENT '商品信息',
    status VARCHAR(32) NOT NULL DEFAULT 'PENDING' COMMENT '订单状态：PENDING/COMPLETED/CANCELLED',
    create_time BIGINT NOT NULL COMMENT '创建时间 (毫秒时间戳)',
    update_time BIGINT COMMENT '更新时间 (毫秒时间戳)',
    
    INDEX idx_user_id (user_id),
    INDEX idx_status (status),
    INDEX idx_create_time (create_time)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='原始订单表';

-- ============================================
-- 2. 历史数据表 (处理结果)
-- ============================================
DROP TABLE IF EXISTS order_history;

CREATE TABLE order_history (
    id BIGINT AUTO_INCREMENT PRIMARY KEY COMMENT '自增主键',
    user_id VARCHAR(64) NOT NULL COMMENT '用户 ID',
    total_amount DECIMAL(10,2) NOT NULL COMMENT '总金额',
    process_time BIGINT NOT NULL COMMENT '处理时间 (毫秒时间戳)',
    status VARCHAR(32) DEFAULT 'PROCESSED' COMMENT '处理状态',
    remark VARCHAR(500) COMMENT '备注信息',
    
    INDEX idx_user_id (user_id),
    INDEX idx_process_time (process_time),
    INDEX idx_status (status)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='订单历史数据表';

-- ============================================
-- 3. 用户信息表 (用于 ES 数据增强参考)
-- ============================================
DROP TABLE IF EXISTS users;

CREATE TABLE users (
    user_id VARCHAR(64) PRIMARY KEY COMMENT '用户 ID',
    username VARCHAR(128) COMMENT '用户名',
    user_level VARCHAR(32) DEFAULT 'NORMAL' COMMENT '用户等级：VIP/NORMAL',
    risk_score INT DEFAULT 0 COMMENT '风险评分：0-100',
    register_time BIGINT COMMENT '注册时间',
    last_login_time BIGINT COMMENT '最后登录时间',
    
    INDEX idx_user_level (user_level),
    INDEX idx_risk_score (risk_score)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='用户信息表';

-- ============================================
-- 4. 插入测试数据
-- ============================================

-- 插入测试用户
INSERT INTO users (user_id, username, user_level, risk_score, register_time, last_login_time) VALUES
('user-1', '张三', 'NORMAL', 30, UNIX_TIMESTAMP() * 1000 - 86400000 * 30, UNIX_TIMESTAMP() * 1000),
('user-2', '李四', 'NORMAL', 75, UNIX_TIMESTAMP() * 1000 - 86400000 * 60, UNIX_TIMESTAMP() * 1000 - 3600000),
('user-3', '王五', 'VIP', 20, UNIX_TIMESTAMP() * 1000 - 86400000 * 90, UNIX_TIMESTAMP() * 1000),
('user-4', '赵六', 'NORMAL', 85, UNIX_TIMESTAMP() * 1000 - 86400000 * 10, UNIX_TIMESTAMP() * 1000 - 7200000),
('user-5', '钱七', 'VIP', 15, UNIX_TIMESTAMP() * 1000 - 86400000 * 120, UNIX_TIMESTAMP() * 1000);

-- 插入测试订单 (待处理的 PENDING 订单)
INSERT INTO orders (order_id, user_id, amount, product, status, create_time) VALUES
('order-001', 'user-1', 199.99, '商品 A', 'PENDING', UNIX_TIMESTAMP() * 1000),
('order-002', 'user-2', 299.99, '商品 B', 'PENDING', UNIX_TIMESTAMP() * 1000),
('order-003', 'user-1', 99.99, '商品 C', 'PENDING', UNIX_TIMESTAMP() * 1000),
('order-004', 'user-3', 1500.00, '商品 D', 'PENDING', UNIX_TIMESTAMP() * 1000),
('order-005', 'user-2', 50.00, '商品 E', 'PENDING', UNIX_TIMESTAMP() * 1000),
('order-006', 'user-4', 899.99, '商品 F', 'PENDING', UNIX_TIMESTAMP() * 1000),
('order-007', 'user-5', 2999.00, '商品 G', 'PENDING', UNIX_TIMESTAMP() * 1000),
('order-008', 'user-1', 450.00, '商品 H', 'PENDING', UNIX_TIMESTAMP() * 1000),
('order-009', 'user-3', 120.00, '商品 I', 'PENDING', UNIX_TIMESTAMP() * 1000),
('order-010', 'user-4', 680.00, '商品 J', 'PENDING', UNIX_TIMESTAMP() * 1000);

-- 插入一些已完成的历史订单
INSERT INTO orders (order_id, user_id, amount, product, status, create_time, update_time) VALUES
('order-101', 'user-1', 350.00, '商品 K', 'COMPLETED', UNIX_TIMESTAMP() * 1000 - 86400000, UNIX_TIMESTAMP() * 1000 - 43200000),
('order-102', 'user-2', 180.00, '商品 L', 'COMPLETED', UNIX_TIMESTAMP() * 1000 - 172800000, UNIX_TIMESTAMP() * 1000 - 86400000),
('order-103', 'user-3', 2200.00, '商品 M', 'COMPLETED', UNIX_TIMESTAMP() * 1000 - 259200000, UNIX_TIMESTAMP() * 1000 - 172800000);

-- ============================================
-- 5. 查询示例
-- ============================================

-- 查看所有待处理的订单
SELECT * FROM orders WHERE status = 'PENDING' ORDER BY create_time DESC;

-- 查看每个用户的订单总数和总金额
SELECT 
    user_id,
    COUNT(*) as order_count,
    SUM(amount) as total_amount
FROM orders 
WHERE status = 'PENDING'
GROUP BY user_id;

-- 查看高风险用户的订单 (风险评分 >= 80)
SELECT o.* 
FROM orders o
JOIN users u ON o.user_id = u.user_id
WHERE o.status = 'PENDING' AND u.risk_score >= 80;

-- 查看 VIP 用户的大额订单
SELECT o.* 
FROM orders o
JOIN users u ON o.user_id = u.user_id
WHERE o.status = 'PENDING' 
  AND u.user_level = 'VIP' 
  AND o.amount > 1000;

-- ============================================
-- 6. 权限设置 (根据实际情况调整)
-- ============================================

-- 创建应用用户 (可选)
-- CREATE USER 'flink_app'@'%' IDENTIFIED BY 'your_password';
-- GRANT SELECT, INSERT, UPDATE ON flink_db.orders TO 'flink_app'@'%';
-- GRANT SELECT, INSERT ON flink_db.order_history TO 'flink_app'@'%';
-- GRANT SELECT ON flink_db.users TO 'flink_app'@'%';
-- FLUSH PRIVILEGES;

-- ============================================
-- 7. 数据清理脚本 (定期执行)
-- ============================================

-- 清理 30 天前的历史数据 (根据需要调整)
-- DELETE FROM order_history 
-- WHERE process_time < UNIX_TIMESTAMP() * 1000 - 2592000000;

-- 归档已完成的订单到历史表
-- INSERT INTO order_history (user_id, total_amount, process_time, status)
-- SELECT user_id, SUM(amount), UNIX_TIMESTAMP() * 1000, 'ARCHIVED'
-- FROM orders
-- WHERE status = 'COMPLETED' 
--   AND create_time < UNIX_TIMESTAMP() * 1000 - 604800000
-- GROUP BY user_id;
