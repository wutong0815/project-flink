package com.example.project.core.minio;

import io.minio.*;
import io.minio.errors.*;
import io.minio.http.Method;
import io.minio.messages.Bucket;
import io.minio.messages.Item;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.InputStream;
import java.util.HashMap;
import java.util.Map;

/**
 * MinIO 客户端工具类
 * 
 * 提供 MinIO S3 兼容存储的基本操作:
 * - Bucket 管理 (创建、删除、列表)
 * - Object 管理 (上传、下载、删除、列表)
 * - Presigned URL 生成
 */
public class MinioClientUtil {
    
    private static final Logger LOG = LoggerFactory.getLogger(MinioClientUtil.class);
    
    private MinioClient minioClient;
    private String endpoint;
    private String accessKey;
    private String secretKey;
    private boolean useHttps;
    
    /**
     * 构造函数
     * 
     * @param endpoint MinIO 服务器地址，例如：http://localhost:9000 或 https://play.min.io
     * @param accessKey 访问密钥
     * @param secretKey 秘密密钥
     * @param useHttps 是否使用 HTTPS
     */
    public MinioClientUtil(String endpoint, String accessKey, String secretKey, boolean useHttps) {
        this.endpoint = endpoint;
        this.accessKey = accessKey;
        this.secretKey = secretKey;
        this.useHttps = useHttps;
        initClient();
    }
    
    /**
     * 初始化 MinIO 客户端
     */
    private void initClient() {
        try {
            this.minioClient = MinioClient.builder()
                    .endpoint(endpoint)
                    .credentials(accessKey, secretKey)
                    .build();
            LOG.info("MinIO 客户端初始化成功 - Endpoint: {}", endpoint);
        } catch (Exception e) {
            LOG.error("MinIO 客户端初始化失败", e);
            throw new RuntimeException("MinIO 客户端初始化失败", e);
        }
    }
    
    /**
     * 检查 bucket 是否存在
     * 
     * @param bucketName bucket 名称
     * @return true 如果存在
     */
    public boolean bucketExists(String bucketName) {
        try {
            return minioClient.bucketExists(BucketExistsArgs.builder().bucket(bucketName).build());
        } catch (Exception e) {
            LOG.error("检查 bucket 失败：{}", bucketName, e);
            throw new RuntimeException("检查 bucket 失败", e);
        }
    }
    
    /**
     * 创建 bucket
     * 
     * @param bucketName bucket 名称
     */
    public void createBucket(String bucketName) {
        try {
            if (!bucketExists(bucketName)) {
                minioClient.makeBucket(MakeBucketArgs.builder().bucket(bucketName).build());
                LOG.info("Bucket 创建成功：{}", bucketName);
            } else {
                LOG.info("Bucket 已存在：{}", bucketName);
            }
        } catch (Exception e) {
            LOG.error("创建 bucket 失败：{}", bucketName, e);
            throw new RuntimeException("创建 bucket 失败", e);
        }
    }
    
    /**
     * 删除 bucket
     * 
     * @param bucketName bucket 名称
     */
    public void removeBucket(String bucketName) {
        try {
            minioClient.removeBucket(RemoveBucketArgs.builder().bucket(bucketName).build());
            LOG.info("Bucket 删除成功：{}", bucketName);
        } catch (Exception e) {
            LOG.error("删除 bucket 失败：{}", bucketName, e);
            throw new RuntimeException("删除 bucket 失败", e);
        }
    }
    
    /**
     * 列出所有 buckets
     * 
     * @return bucket 名称列表
     */
    public java.util.List<Bucket> listBuckets() {
        try {
            return minioClient.listBuckets();
        } catch (Exception e) {
            LOG.error("列出 buckets 失败", e);
            throw new RuntimeException("列出 buckets 失败", e);
        }
    }
    
    /**
     * 上传文件
     * 
     * @param bucketName bucket 名称
     * @param objectName 对象名称 (文件路径)
     * @param inputStream 输入流
     * @param contentType 内容类型 (可选，null 表示自动检测)
     */
    public void putObject(String bucketName, String objectName, InputStream inputStream, String contentType) {
        try {
            PutObjectArgs.Builder builder = PutObjectArgs.builder()
                    .bucket(bucketName)
                    .object(objectName)
                    .stream(inputStream, -1, 10 * 1024 * 1024); // 最大分片大小 10MB
            
            if (contentType != null && !contentType.isEmpty()) {
                builder.contentType(contentType);
            }
            
            minioClient.putObject(builder.build());
            LOG.info("文件上传成功：{}/{}", bucketName, objectName);
        } catch (Exception e) {
            LOG.error("文件上传失败：{}/{}", bucketName, objectName, e);
            throw new RuntimeException("文件上传失败", e);
        }
    }
    
    /**
     * 下载文件
     * 
     * @param bucketName bucket 名称
     * @param objectName 对象名称
     * @return 输入流
     */
    public InputStream getObject(String bucketName, String objectName) {
        try {
            return minioClient.getObject(GetObjectArgs.builder()
                    .bucket(bucketName)
                    .object(objectName)
                    .build());
        } catch (Exception e) {
            LOG.error("文件下载失败：{}/{}", bucketName, objectName, e);
            throw new RuntimeException("文件下载失败", e);
        }
    }
    
    /**
     * 删除文件
     * 
     * @param bucketName bucket 名称
     * @param objectName 对象名称
     */
    public void removeObject(String bucketName, String objectName) {
        try {
            minioClient.removeObject(RemoveObjectArgs.builder()
                    .bucket(bucketName)
                    .object(objectName)
                    .build());
            LOG.info("文件删除成功：{}/{}", bucketName, objectName);
        } catch (Exception e) {
            LOG.error("文件删除失败：{}/{}", bucketName, objectName, e);
            throw new RuntimeException("文件删除失败", e);
        }
    }
    
    /**
     * 列出 bucket 中的对象
     * 
     * @param bucketName bucket 名称
     * @param prefix 前缀过滤 (可选)
     * @param recursive 是否递归列出
     * @return 可迭代的结果
     */
    public Iterable<Result<Item>> listObjects(String bucketName, String prefix, boolean recursive) {
        try {
            ListObjectsArgs.Builder builder = ListObjectsArgs.builder()
                    .bucket(bucketName);
            
            if (prefix != null && !prefix.isEmpty()) {
                builder.prefix(prefix);
            }
            builder.recursive(recursive);
            
            return minioClient.listObjects(builder.build());
        } catch (Exception e) {
            LOG.error("列出对象失败：{}", bucketName, e);
            throw new RuntimeException("列出对象失败", e);
        }
    }
    
    /**
     * 获取对象的元数据
     * 
     * @param bucketName bucket 名称
     * @param objectName 对象名称
     * @return 对象统计信息
     */
    public StatObjectResponse statObject(String bucketName, String objectName) {
        try {
            return minioClient.statObject(StatObjectArgs.builder()
                    .bucket(bucketName)
                    .object(objectName)
                    .build());
        } catch (Exception e) {
            LOG.error("获取对象元数据失败：{}/{}", bucketName, objectName, e);
            throw new RuntimeException("获取对象元数据失败", e);
        }
    }
    
    /**
     * 生成预签名 URL (用于临时访问)
     * 
     * @param bucketName bucket 名称
     * @param objectName 对象名称
     * @param method HTTP 方法 (GET, PUT 等)
     * @param expirySeconds 过期时间 (秒)
     * @return 预签名 URL
     */
    public String getPresignedObjectUrl(String bucketName, String objectName, Method method, int expirySeconds) {
        try {
            return minioClient.getPresignedObjectUrl(GetPresignedObjectUrlArgs.builder()
                    .method(method)
                    .bucket(bucketName)
                    .object(objectName)
                    .expiry(expirySeconds)
                    .build());
        } catch (Exception e) {
            LOG.error("生成预签名 URL 失败：{}/{}", bucketName, objectName, e);
            throw new RuntimeException("生成预签名 URL 失败", e);
        }
    }
    
    /**
     * 关闭客户端
     */
    public void closeClient() {
        // MinIO Client 不需要显式关闭，它会自动管理连接
        minioClient = null;
        LOG.info("MinIO 客户端已释放");
    }
}
