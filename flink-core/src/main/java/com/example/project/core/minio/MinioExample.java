package com.example.project.core.minio;

import io.minio.Result;
import io.minio.messages.Bucket;
import io.minio.messages.Item;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayInputStream;
import java.nio.charset.StandardCharsets;

/**
 * MinIO 客户端使用示例
 */
public class MinioExample {
    
    private static final Logger LOG = LoggerFactory.getLogger(MinioExample.class);
    
    // MinIO 配置 (请根据实际情况修改)
    private static final String ENDPOINT = "http://localhost:9000";
    private static final String ACCESS_KEY = "minioadmin";
    private static final String SECRET_KEY = "minioadmin";
    private static final boolean USE_HTTPS = false;
    
    /**
     * 运行 MinIO 示例
     */
    public void runExample() {
        MinioClientUtil minioClient = null;
        try {
            // 1. 初始化客户端
            minioClient = new MinioClientUtil(ENDPOINT, ACCESS_KEY, SECRET_KEY, USE_HTTPS);
            
            // 2. 创建 bucket
            String bucketName = "test-bucket";
            minioClient.createBucket(bucketName);
            
            // 3. 列出所有 buckets
            LOG.info("=== 所有 Buckets ===");
            for (Bucket bucket : minioClient.listBuckets()) {
                LOG.info("Bucket: {} - 创建时间：{}", bucket.name(), bucket.creationDate());
            }
            
            // 4. 上传文件
            String objectName = "test-file.txt";
            String content = "Hello, MinIO! This is a test file.";
            ByteArrayInputStream inputStream = new ByteArrayInputStream(
                    content.getBytes(StandardCharsets.UTF_8)
            );
            minioClient.putObject(bucketName, objectName, inputStream, "text/plain");
            LOG.info("文件上传成功：{}/{}", bucketName, objectName);
            
            // 5. 下载文件
            LOG.info("\n=== 下载的文件内容 ===");
            try (java.io.InputStream downloadedStream = minioClient.getObject(bucketName, objectName)) {
                String downloadedContent = new String(downloadedStream.readAllBytes(), StandardCharsets.UTF_8);
                LOG.info("下载的内容：{}", downloadedContent);
            }
            
            // 6. 获取对象元数据
            LOG.info("\n=== 对象元数据 ===");
            var statObj = minioClient.statObject(bucketName, objectName);
            LOG.info("对象大小：{} bytes", statObj.size());
            LOG.info("内容类型：{}", statObj.contentType());
            LOG.info("最后修改时间：{}", statObj.lastModified());
            
            // 7. 列出 bucket 中的对象
            LOG.info("\n=== Bucket 中的对象 ===");
            for (Result<Item> result : minioClient.listObjects(bucketName, "", true)) {
                Item item = result.get();
                LOG.info("对象：{} - 大小：{} bytes", item.objectName(), item.size());
            }
            
            // 8. 生成预签名 URL (有效期 7 天)
            LOG.info("\n=== 预签名 URL ===");
            String presignedUrl = minioClient.getPresignedObjectUrl(
                    bucketName, 
                    objectName, 
                    io.minio.http.Method.GET, 
                    604800 // 7 天
            );
            LOG.info("GET 预签名 URL: {}", presignedUrl);
            
            // 9. 删除文件
            minioClient.removeObject(bucketName, objectName);
            LOG.info("\n文件已删除：{}/{}", bucketName, objectName);
            
            // 10. 删除 bucket
            minioClient.removeBucket(bucketName);
            LOG.info("Bucket 已删除：{}", bucketName);
            
            LOG.info("\n=== MinIO 示例执行完成 ===");
            
        } catch (Exception e) {
            LOG.error("MinIO 示例执行失败", e);
            throw new RuntimeException("MinIO 示例执行失败", e);
        } finally {
            // 关闭客户端
            if (minioClient != null) {
                minioClient.closeClient();
            }
        }
    }
    
    /**
     * 主方法 - 用于独立运行
     */
    public static void main(String[] args) {
        MinioExample example = new MinioExample();
        example.runExample();
    }
}
