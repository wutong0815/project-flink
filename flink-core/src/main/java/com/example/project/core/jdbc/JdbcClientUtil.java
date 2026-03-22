package com.example.project.core.jdbc;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.*;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Consumer;

/**
 * JDBC 数据库连接工具类
 * 
 * 提供数据库连接管理、批量插入、查询等功能
 * 支持 MySQL、PostgreSQL 等关系型数据库
 */
public class JdbcClientUtil {
    
    private static final Logger LOG = LoggerFactory.getLogger(JdbcClientUtil.class);
    
    private String jdbcUrl;
    private String username;
    private String password;
    private String driverClassName;
    
    /**
     * 构造函数
     * 
     * @param jdbcUrl JDBC 连接 URL，例如：jdbc:mysql://localhost:3306/mydb
     * @param username 数据库用户名
     * @param password 数据库密码
     * @param driverClassName 驱动类名，例如：com.mysql.cj.jdbc.Driver
     */
    public JdbcClientUtil(String jdbcUrl, String username, String password, String driverClassName) {
        this.jdbcUrl = jdbcUrl;
        this.username = username;
        this.password = password;
        this.driverClassName = driverClassName;
        
        // 加载数据库驱动
        try {
            Class.forName(driverClassName);
            LOG.info("数据库驱动加载成功：{}", driverClassName);
        } catch (ClassNotFoundException e) {
            LOG.error("数据库驱动加载失败：{}", driverClassName, e);
            throw new RuntimeException("数据库驱动加载失败", e);
        }
        
        LOG.info("JDBC 客户端初始化成功 - URL: {}, Username: {}", jdbcUrl, username);
    }
    
    /**
     * 获取数据库连接
     * 
     * @return Connection 对象
     * @throws SQLException SQL 异常
     */
    public Connection getConnection() throws SQLException {
        return DriverManager.getConnection(jdbcUrl, username, password);
    }
    
    /**
     * 测试数据库连接是否有效
     * 
     * @return true 如果连接有效
     */
    public boolean isValidConfig() {
        try (Connection conn = getConnection()) {
            boolean valid = conn.isValid(5);
            if (valid) {
                LOG.info("数据库连接测试通过 ✓");
            } else {
                LOG.error("数据库连接测试失败 ✗");
            }
            return valid;
        } catch (SQLException e) {
            LOG.error("数据库连接测试失败", e);
            return false;
        }
    }
    
    /**
     * 批量插入数据
     * 
     * @param tableName 表名
     * @param columns 列名列表
     * @param valuesList 值列表
     * @throws Exception 异常
     */
    public void batchInsert(String tableName, List<String> columns, List<List<Object>> valuesList) throws Exception {
        if (valuesList.isEmpty()) {
            LOG.warn("没有数据需要插入");
            return;
        }
        
        StringBuilder sqlBuilder = new StringBuilder();
        sqlBuilder.append("INSERT INTO ").append(tableName).append(" (");
        sqlBuilder.append(String.join(", ", columns));
        sqlBuilder.append(") VALUES (");
        sqlBuilder.append(String.join(", ", columns.stream().map(c -> "?").toArray(String[]::new)));
        sqlBuilder.append(")");
        
        String sql = sqlBuilder.toString();
        LOG.info("执行批量插入：{}, 数据量：{}", sql, valuesList.size());
        
        try (Connection conn = getConnection();
             PreparedStatement pstmt = conn.prepareStatement(sql)) {
            
            conn.setAutoCommit(false);
            int batchSize = 100;
            int count = 0;
            
            for (List<Object> values : valuesList) {
                for (int i = 0; i < values.size(); i++) {
                    pstmt.setObject(i + 1, values.get(i));
                }
                pstmt.addBatch();
                
                if (++count % batchSize == 0) {
                    pstmt.executeBatch();
                }
            }
            
            pstmt.executeBatch();
            conn.commit();
            
            LOG.info("批量插入成功，共插入 {} 条记录", valuesList.size());
        }
    }
    
    /**
     * 执行查询并返回结果列表
     * 
     * @param sql SQL 语句
     * @param params 参数列表
     * @return 结果列表，每行是一个 Map
     * @throws Exception 异常
     */
    public List<Map<String, Object>> query(String sql, Object... params) throws Exception {
        LOG.debug("执行查询：{}, 参数：{}", sql, params);
        
        List<Map<String, Object>> results = new ArrayList<>();
        
        try (Connection conn = getConnection();
             PreparedStatement pstmt = conn.prepareStatement(sql)) {
            
            for (int i = 0; i < params.length; i++) {
                pstmt.setObject(i + 1, params[i]);
            }
            
            try (ResultSet rs = pstmt.executeQuery()) {
                ResultSetMetaData metaData = rs.getMetaData();
                int columnCount = metaData.getColumnCount();
                
                while (rs.next()) {
                    Map<String, Object> row = new HashMap<>();
                    for (int i = 1; i <= columnCount; i++) {
                        String columnName = metaData.getColumnLabel(i);
                        row.put(columnName, rs.getObject(i));
                    }
                    results.add(row);
                }
            }
        }
        
        LOG.debug("查询返回 {} 条记录", results.size());
        return results;
    }
    
    /**
     * 执行更新操作 (INSERT/UPDATE/DELETE)
     * 
     * @param sql SQL 语句
     * @param params 参数列表
     * @return 影响的行数
     * @throws Exception 异常
     */
    public int update(String sql, Object... params) throws Exception {
        LOG.debug("执行更新：{}, 参数：{}", sql, params);
        
        try (Connection conn = getConnection();
             PreparedStatement pstmt = conn.prepareStatement(sql)) {
            
            for (int i = 0; i < params.length; i++) {
                pstmt.setObject(i + 1, params[i]);
            }
            
            int rows = pstmt.executeUpdate();
            LOG.debug("更新成功，影响 {} 行", rows);
            return rows;
        }
    }
    
    /**
     * 使用事务执行批量操作
     * 
     * @param operations 操作列表，每个操作包含 SQL 和参数
     * @throws Exception 异常
     */
    public void executeInTransaction(List<DbOperation> operations) throws Exception {
        try (Connection conn = getConnection()) {
            conn.setAutoCommit(false);
            
            try {
                for (DbOperation operation : operations) {
                    try (PreparedStatement pstmt = conn.prepareStatement(operation.sql)) {
                        for (int i = 0; i < operation.params.length; i++) {
                            pstmt.setObject(i + 1, operation.params[i]);
                        }
                        pstmt.executeUpdate();
                    }
                }
                
                conn.commit();
                LOG.info("事务执行成功，共 {} 个操作", operations.size());
                
            } catch (Exception e) {
                conn.rollback();
                LOG.error("事务执行失败，已回滚", e);
                throw e;
            } finally {
                conn.setAutoCommit(true);
            }
        }
    }
    
    /**
     * 数据库操作接口
     */
    public interface DbOperation {
        void execute(Connection conn) throws SQLException;
    }
    
    /**
     * 获取 JDBC URL
     */
    public String getJdbcUrl() {
        return jdbcUrl;
    }
    
    /**
     * 关闭资源 (可选，推荐使用 try-with-resources)
     */
    public void close() {
        // 连接池会自动管理连接，不需要手动关闭
        LOG.info("JDBC 客户端资源已释放");
    }
}
