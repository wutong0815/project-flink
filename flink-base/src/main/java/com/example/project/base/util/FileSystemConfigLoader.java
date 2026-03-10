package com.example.project.base.util;

import org.apache.flink.configuration.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 文件系统配置加载工具类
 * 
 * 用于从 application.properties 加载和初始化文件系统相关配置:
 * - MinIO/S3 连接配置
 * - S3A 文件系统配置
 * - Checkpoint 和 Savepoint 目录配置
 */
public class FileSystemConfigLoader {
    
    private static final Logger LOG = LoggerFactory.getLogger(FileSystemConfigLoader.class);
    
    // MinIO 配置键
    public static final String MINIO_ENDPOINT = "minio.endpoint";
    public static final String MINIO_ACCESS_KEY = "minio.access-key";
    public static final String MINIO_SECRET_KEY = "minio.secret-key";
    public static final String MINIO_USE_HTTPS = "minio.use-https";
    
    // S3A 配置键
    public static final String S3A_ENDPOINT = "s3a.endpoint";
    public static final String S3A_ACCESS_KEY = "s3a.access-key";
    public static final String S3A_SECRET_KEY = "s3a.secret-key";
    public static final String S3A_PATH_STYLE_ACCESS = "s3a.path.style.access";
    public static final String S3A_CONNECTION_SSL_ENABLED = "s3a.connection.ssl.enabled";
    
    // 状态后端配置键
    public static final String STATE_CHECKPOINTS_DIR = "state.checkpoints.dir";
    public static final String STATE_SAVEPOINTS_DIR = "state.savepoints.dir";
    
    // Hadoop S3A 配置键 (用于 flink-conf.yaml)
    public static final String FS_S3A_IMPL = "fs.s3a.impl";
    public static final String FS_S3A_AWS_CREDENTIALS_PROVIDER = "fs.s3a.aws.credentials.provider";
    public static final String FS_S3A_ENDPOINT = "fs.s3a.endpoint";
    public static final String FS_S3A_ACCESS_KEY = "fs.s3a.access.key";
    public static final String FS_S3A_SECRET_KEY = "fs.s3a.secret.key";
    public static final String FS_S3A_PATH_STYLE_ACCESS = "fs.s3a.path.style.access";
    public static final String FS_S3A_CONNECTION_SSL_ENABLED = "fs.s3a.connection.ssl.enabled";
    
    /**
     * 从系统属性加载文件系统配置并应用到 Flink 配置
     * 
     * @param flinkConfig Flink 配置对象
     * @return 加载后的 Flink 配置
     */
    public static Configuration loadFileSystemConfig(Configuration flinkConfig) {
        LOG.info("开始加载文件系统配置...");
        
        try {
            // 1. 加载 MinIO 基础配置
            String endpoint = getSystemProperty(MINIO_ENDPOINT, "http://minio:9000");
            String accessKey = getSystemProperty(MINIO_ACCESS_KEY, "minioadmin");
            String secretKey = getSystemProperty(MINIO_SECRET_KEY, "minioadmin");
            boolean useHttps = Boolean.parseBoolean(
                getSystemProperty(MINIO_USE_HTTPS, "false")
            );
            
            LOG.info("MinIO 配置 - Endpoint: {}, UseHttps: {}", endpoint, useHttps);
            
            // 2. 配置 S3A 文件系统参数
            String s3aEndpoint = getSystemProperty(S3A_ENDPOINT, endpoint);
            String s3aAccessKey = getSystemProperty(S3A_ACCESS_KEY, accessKey);
            String s3aSecretKey = getSystemProperty(S3A_SECRET_KEY, secretKey);
            boolean pathStyleAccess = Boolean.parseBoolean(
                getSystemProperty(S3A_PATH_STYLE_ACCESS, "true")
            );
            boolean sslEnabled = Boolean.parseBoolean(
                getSystemProperty(S3A_CONNECTION_SSL_ENABLED, "false")
            );
            
            // 3. 应用 S3A 配置到 Flink
            flinkConfig.setString(FS_S3A_IMPL, "org.apache.hadoop.fs.s3a.S3AFileSystem");
            flinkConfig.setString(FS_S3A_AWS_CREDENTIALS_PROVIDER, 
                "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider");
            flinkConfig.setString(FS_S3A_ENDPOINT, s3aEndpoint);
            flinkConfig.setString(FS_S3A_ACCESS_KEY, s3aAccessKey);
            flinkConfig.setString(FS_S3A_SECRET_KEY, s3aSecretKey);
            flinkConfig.setBoolean(FS_S3A_PATH_STYLE_ACCESS, pathStyleAccess);
            flinkConfig.setBoolean(FS_S3A_CONNECTION_SSL_ENABLED, sslEnabled);
            
            LOG.info("S3A 文件系统配置完成");
            
            // 4. 配置 Checkpoint 和 Savepoint 目录
            String checkpointsDir = getSystemProperty(STATE_CHECKPOINTS_DIR, "s3a://flink-checkpoints/");
            String savepointsDir = getSystemProperty(STATE_SAVEPOINTS_DIR, "s3a://flink-savepoints/");
            
            flinkConfig.setString("state.checkpoints.dir", checkpointsDir);
            flinkConfig.setString("state.savepoints.dir", savepointsDir);
            
            LOG.info("状态后端目录配置完成 - Checkpoints: {}, Savepoints: {}", 
                checkpointsDir, savepointsDir);
            
            // 5. 启用文件系统安全策略（可选）
            flinkConfig.setBoolean("fs.s3a.impl.disable.cache", true);
            
            LOG.info("文件系统配置加载成功");
            
        } catch (Exception e) {
            LOG.error("文件系统配置加载失败", e);
            throw new RuntimeException("文件系统配置加载失败", e);
        }
        
        return flinkConfig;
    }
    
    /**
     * 获取系统属性，如果不存在则返回默认值
     * 
     * @param key 属性键
     * @param defaultValue 默认值
     * @return 属性值或默认值
     */
    private static String getSystemProperty(String key, String defaultValue) {
        String value = System.getProperty(key);
        if (value == null || value.trim().isEmpty()) {
            value = System.getenv(key.replace('.', '_').toUpperCase());
        }
        if (value == null || value.trim().isEmpty()) {
            value = defaultValue;
        }
        return value;
    }
    
    /**
     * 创建默认的 Flink 配置并加载文件系统配置
     * 
     * @return 配置好的 Flink 配置对象
     */
    public static Configuration createDefaultConfig() {
        Configuration config = new Configuration();
        return loadFileSystemConfig(config);
    }
}
