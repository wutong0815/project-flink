package com.example.project.core.jdbc;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.List;
import java.util.Map;

/**
 * JDBC 数据库操作示例
 * 
 * 演示如何使用 JdbcClientUtil 进行数据库操作
 */
public class JdbcExample {
    
    private static final Logger LOG = LoggerFactory.getLogger(JdbcExample.class);
    
    /**
     * 运行数据库示例
     */
    public void runExample() {
        try {
            LOG.info("========================================");
            LOG.info("JDBC 数据库客户端使用示例");
            LOG.info("========================================");
            
            // 1. 创建数据库客户端 (以 MySQL 为例)
            String jdbcUrl = "jdbc:mysql://localhost:3306/flink_db?useSSL=false&serverTimezone=UTC&characterEncoding=utf8";
            String username = "root";
            String password = "password";
            String driverClass = "com.mysql.cj.jdbc.Driver";
            
            LOG.info("步骤 1: 创建 JDBC 客户端");
            LOG.info("JDBC URL: {}", jdbcUrl);
            LOG.info("Username: {}", username);
            
            JdbcClientUtil jdbcClient = new JdbcClientUtil(jdbcUrl, username, password, driverClass);
            
            // 2. 测试连接
            LOG.info("\n步骤 2: 测试数据库连接");
            if (jdbcClient.isValidConfig()) {
                LOG.info("数据库连接测试通过 ✓");
            } else {
                LOG.error("数据库连接测试失败 ✗");
                return;
            }
            
            // 3. 示例：查询数据
            LOG.info("\n步骤 3: 查询数据示例");
            String querySql = "SELECT * FROM orders WHERE user_id = ? AND amount > ?";
            List<Map<String, Object>> results = jdbcClient.query(querySql, "user-1", 100.0);
            
            LOG.info("查询结果：{} 条记录", results.size());
            for (Map<String, Object> row : results) {
                LOG.info("  - {}", row);
            }
            
            // 4. 示例：批量插入数据
            LOG.info("\n步骤 4: 批量插入数据示例");
            String tableName = "order_history";
            List<String> columns = Arrays.asList("order_id", "user_id", "amount", "product", "status", "create_time");
            
            List<List<Object>> valuesList = Arrays.asList(
                Arrays.asList("order-001", "user-1", 199.99, "Product A", "COMPLETED", System.currentTimeMillis()),
                Arrays.asList("order-002", "user-2", 299.99, "Product B", "PENDING", System.currentTimeMillis()),
                Arrays.asList("order-003", "user-1", 99.99, "Product C", "COMPLETED", System.currentTimeMillis())
            );
            
            jdbcClient.batchInsert(tableName, columns, valuesList);
            LOG.info("批量插入完成");
            
            // 5. 示例：执行更新操作
            LOG.info("\n步骤 5: 执行更新操作");
            String updateSql = "UPDATE order_history SET status = ? WHERE order_id = ?";
            int rows = jdbcClient.update(updateSql, "SHIPPED", "order-001");
            LOG.info("更新成功，影响 {} 行", rows);
            
            // 6. 使用指南
            LOG.info("\n========================================");
            LOG.info("使用指南:");
            LOG.info("========================================");
            LOG.info("1. 查询数据:");
            LOG.info("   List<Map<String, Object>> results = jdbcClient.query(sql, params);");
            LOG.info("");
            LOG.info("2. 批量插入:");
            LOG.info("   jdbcClient.batchInsert(tableName, columns, valuesList);");
            LOG.info("");
            LOG.info("3. 执行更新:");
            LOG.info("   int rows = jdbcClient.update(sql, params);");
            LOG.info("");
            LOG.info("4. 事务操作:");
            LOG.info("   jdbcClient.executeInTransaction(operations);");
            LOG.info("");
            LOG.info("========================================");
            LOG.info("示例运行完成！");
            LOG.info("========================================");
            
        } catch (Exception e) {
            LOG.error("运行 JDBC 示例时发生错误", e);
            throw new RuntimeException("JDBC 示例运行失败", e);
        }
    }
    
    /**
     * 主方法 - 用于独立运行
     */
    public static void main(String[] args) {
        JdbcExample example = new JdbcExample();
        example.runExample();
    }
}
