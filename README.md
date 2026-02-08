# Flink Savepoint with MinIO Integration

这是一个演示如何使用MinIO作为后端存储来实现Flink savepoint功能的项目。

## 功能特性

- 使用MinIO作为Flink检查点和savepoint的存储后端
- 通过Hadoop S3A文件系统访问MinIO
- REST API用于触发savepoint操作
- Docker Compose一键部署环境

## 技术栈

- Apache Flink 1.17.1
- MinIO (兼容S3的对象存储)
- Hadoop AWS S3A文件系统
- Docker & Docker Compose

## 快速开始

### 1. 构建项目

```bash
mvn clean package
```

### 2. 启动服务

```bash
docker-compose up -d
```

这将启动以下服务：
- MinIO对象存储
- Flink JobManager
- Flink TaskManager

### 3. 访问服务

- Flink Web UI: http://localhost:8081
- MinIO Console: http://localhost:9001 (用户名: minioadmin, 密码: minioadmin)

## 配置说明

### Flink配置

[flink-conf.yaml](assembles/flink-install/flink/conf/flink-conf.yaml) 包含了以下关键配置：

```yaml
# 检查点和savepoint存储位置
state.checkpoints.dir: s3a://flink-checkpoints/
state.savepoints.dir: s3a://flink-savepoints/

# S3A文件系统配置
fs.s3a.impl: org.apache.hadoop.fs.s3a.S3AFileSystem
fs.s3a.aws.credentials.provider: org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider
fs.s3a.access.key: minioadmin
fs.s3a.secret.key: minioadmin
fs.s3a.endpoint: http://minio:9000
fs.s3a.path.style.access: true
fs.s3a.connection.ssl.enabled: false
```

### Hadoop配置

[core-site.xml](core-site.xml) 定义了S3A文件系统的具体参数：

```xml
<configuration>
    <property>
        <name>fs.s3a.impl</name>
        <value>org.apache.hadoop.fs.s3a.S3AFileSystem</value>
    </property>
    <property>
        <name>fs.s3a.aws.credentials.provider</name>
        <value>org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider</value>
    </property>
    <property>
        <name>fs.s3a.access.key</name>
        <value>minioadmin</value>
    </property>
    <property>
        <name>fs.s3a.secret.key</name>
        <value>minioadmin</value>
    </property>
    <property>
        <name>fs.s3a.endpoint</name>
        <value>http://minio:9000</value>
    </property>
    <property>
        <name>fs.s3a.path.style.access</name>
        <value>true</value>
    </property>
</configuration>
```

## 使用方法

### 运行Flink作业

```bash
# 直接运行作业
flink run -c com.example.project.flinktool.utils.SavepointExample target/flink-savepoint-demo-1.0-SNAPSHOT.jar

# 从savepoint恢复作业
flink run -s <savepoint-path> -c com.example.project.flinktool.utils.SavepointExample target/flink-savepoint-demo-1.0-SNAPSHOT.jar
```

### 通过REST API触发Savepoint

#### 触发Savepoint

```bash
curl -X POST http://localhost:8081/api/v1/savepoint/trigger \
  -H "Content-Type: application/json" \
  -d '{
    "jobId": "<job-id>",
    "savepointPath": "s3a://flink-savepoints/"
  }'
```

#### 停止作业并创建Savepoint

```bash
curl -X POST http://localhost:8081/api/v1/savepoint/stop-with-savepoint \
  -H "Content-Type: application/json" \
  -d '{
    "jobId": "<job-id>",
    "savepointPath": "s3a://flink-savepoints/"
  }'
```

## 项目结构

```
project-flink/
├── src/main/java/com/example/flink/savepoint/
│   ├── SavepointExample.java      # 示例Flink作业
│   ├── SavepointUtil.java         # Savepoint工具类
│   └── controller/
│       └── SavepointController.java # REST API控制器
├── core-site.xml                  # Hadoop配置
├── flink-conf.yaml                # Flink配置
├── Dockerfile                     # Docker镜像定义
├── docker-compose.yml             # Docker Compose配置
├── assembles/                     # 组件装配配置
└── pom.xml                        # Maven依赖管理
```

## 故障排除

1. **无法连接MinIO**：确认MinIO服务正在运行，且网络配置正确
2. **权限问题**：检查MinIO上的bucket权限设置
3. **Hadoop JAR缺失**：确保hadoop-aws和相关依赖已正确添加到Flink的lib目录

## 安全考虑

在生产环境中，请注意：

- 更改默认的MinIO凭据
- 使用HTTPS连接MinIO
- 配置适当的访问控制策略
- 定期备份重要的savepoint数据