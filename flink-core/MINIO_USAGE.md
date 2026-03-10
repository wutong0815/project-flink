# Flink Core - MinIO 客户端使用指南

## 功能概述

flink-core 模块集成了 MinIO S3 兼容存储客户端，提供以下功能:

### MinioClientUtil 工具类

**核心功能:**
- ✅ Bucket 管理 (创建、删除、列表)
- ✅ Object 管理 (上传、下载、删除、列表)
- ✅ 对象元数据查询
- ✅ 预签名 URL 生成

**主要方法:**
```java
// Bucket 操作
boolean bucketExists(String bucketName)
void createBucket(String bucketName)
void removeBucket(String bucketName)
List<Bucket> listBuckets()

// Object 操作
void putObject(String bucketName, String objectName, InputStream inputStream, String contentType)
InputStream getObject(String bucketName, String objectName)
void removeObject(String bucketName, String objectName)
Iterable<Result<Item>> listObjects(String bucketName, String prefix, boolean recursive)
StatObjectResponse statObject(String bucketName, String objectName)

// 预签名 URL
String getPresignedObjectUrl(String bucketName, String objectName, Method method, int expirySeconds)
```

## 使用方法

### 1. 运行完整示例

```bash
cd flink-core
mvn spring-boot:run
# 或
java -jar target/flinkcore-0.0.1-SNAPSHOT.jar
```

示例会执行以下操作:
1. 创建测试 bucket
2. 上传测试文件
3. 下载并显示文件内容
4. 获取对象元数据
5. 列出 bucket 中的对象
6. 生成预签名 URL
7. 清理测试数据

### 2. 仅初始化客户端

```bash
mvn spring-boot:run --args="minio"
```

### 3. 在代码中使用

```java
// 初始化客户端
MinioClientUtil minioClient = new MinioClientUtil(
    "http://localhost:9000",  // MinIO 地址
    "minioadmin",              // Access Key
    "minioadmin",              // Secret Key
    false                      // 是否使用 HTTPS
);

// 创建 bucket
minioClient.createBucket("my-bucket");

// 上传文件
String content = "Hello MinIO!";
InputStream inputStream = new ByteArrayInputStream(content.getBytes());
minioClient.putObject("my-bucket", "test.txt", inputStream, "text/plain");

// 下载文件
try (InputStream downloaded = minioClient.getObject("my-bucket", "test.txt")) {
    String downloadedContent = new String(downloaded.readAllBytes());
    System.out.println("下载的内容：" + downloadedContent);
}

// 列对象
for (Item item : minioClient.listObjects("my-bucket", "", true)) {
    System.out.println("对象：" + item.objectName() + " - 大小：" + item.size());
}

// 释放资源
minioClient.closeClient();
```

## 配置说明

### MinIO 连接参数

| 参数 | 说明 | 默认值 |
|------|------|--------|
| endpoint | MinIO 服务器地址 | http://localhost:9000 |
| accessKey | 访问密钥 | minioadmin |
| secretKey | 秘密密钥 | minioadmin |
| useHttps | 是否使用 HTTPS | false |

### 本地测试环境

使用 Docker Compose 快速启动 MinIO:

```yaml
version: '3'
services:
  minio:
    image: minio/minio
    ports:
      - "9000:9000"
      - "9001:9001"
    environment:
      MINIO_ROOT_USER: minioadmin
      MINIO_ROOT_PASSWORD: minioadmin
    command: server /data --console-address ":9001"
```

启动命令:
```bash
docker-compose up -d
```

访问 MinIO Console: http://localhost:9001

## 注意事项

1. **连接管理**: MinIO Client 内部自动管理连接池，不需要手动关闭
2. **异常处理**: 所有方法都包装了 RuntimeException，便于使用
3. **类型安全**: 使用显式类型声明避免泛型推断问题
4. **资源释放**: 使用完客户端后调用 `closeClient()` 方法释放资源

## 依赖版本

- MinIO SDK: 8.5.7
- OkHttp: 4.12.0
- Flink: 1.17.1
- Java: 21
