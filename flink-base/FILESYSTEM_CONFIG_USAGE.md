# FlinkBase 文件系统配置使用指南

## 📋 概述

`flink-base` 模块现已集成文件系统配置自动加载功能，在启动时会自动从配置文件加载 MinIO/S3 相关配置，并应用到 Flink 执行环境中。

## 🔧 配置项说明

### 1. application.properties 配置

在 `flink-base/src/main/resources/application.properties` 中配置以下参数：

```properties
#==============================================================================
# MinIO / S3 文件系统配置
#==============================================================================

# MinIO 服务器配置
minio.endpoint=http://minio:9000
minio.access-key=minioadmin
minio.secret-key=minioadmin
minio.use-https=false

# S3A 文件系统配置 (用于 Flink 状态后端)
s3a.endpoint=http://minio:9000
s3a.access-key=minioadmin
s3a.secret-key=minioadmin
s3a.path.style.access=true
s3a.connection.ssl.enabled=false

# Checkpoint 和 Savepoint 目录
state.checkpoints.dir=s3a://flink-checkpoints/
state.savepoints.dir=s3a://flink-savepoints/
```

### 2. 环境变量覆盖（可选）

可以通过环境变量覆盖默认配置：

```bash
export MINIO_ENDPOINT=http://minio:9000
export MINIO_ACCESS_KEY=minioadmin
export MINIO_SECRET_KEY=minioadmin
export STATE_CHECKPOINTS_DIR=s3a://flink-checkpoints/
export STATE_SAVEPOINTS_DIR=s3a://flink-savepoints/
```

## 🚀 使用方式

### 方式 1: 自动加载配置（推荐）

直接运行工作流，系统会自动加载文件系统配置：

```java
// FlinkBase 启动时自动加载配置
public class FlinkBase implements CommandLineRunner {
    @Override
    public void run(String... args) throws Exception {
        // 自动初始化文件系统配置
        Configuration flinkConfig = FileSystemConfigLoader.createDefaultConfig();
        
        // 运行工作流（会自动使用配置）
        BasicWorkflow workflow = new BasicWorkflow();
        workflow.runWorkflow(); // 内部会自动加载配置
    }
}
```

### 方式 2: 手动传递配置

如果需要更精细的控制，可以手动传递配置：

```java
import com.example.project.base.util.FileSystemConfigLoader;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

// 加载配置
Configuration config = FileSystemConfigLoader.createDefaultConfig();

// 创建执行环境
StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(config);

// 运行工作流
BasicWorkflow workflow = new BasicWorkflow();
workflow.runWorkflow(config);
```

### 方式 3: 查看当前配置

使用 `config` 命令查看已加载的配置：

```bash
# 运行 FlinkBase 并查看配置
java -jar flinkbase.jar config
```

输出示例：
```
初始化文件系统配置...
文件系统配置已加载:
  - Checkpoints 目录：s3a://flink-checkpoints/
  - Savepoints 目录：s3a://flink-savepoints/
  - S3A Endpoint: http://minio:9000
  - S3A Path Style Access: true
使用方法:
  'basic' - 运行基础工作流
  'kafka [kafkaServers] [inputTopic] [outputTopic]' - 运行 Kafka 工作流
  'config' - 显示当前配置
```

## 📝 命令行参数

### FlinkBase 支持的参数

```bash
# 运行基础工作流（默认）
java -jar flinkbase.jar
# 或
java -jar flinkbase.jar basic

# 运行 Kafka 工作流
java -jar flinkbase.jar kafka localhost:9092 orders order-aggregates

# 查看配置信息
java -jar flinkbase.jar config
```

### 工作流类的使用

#### BasicWorkflow

```java
// 方式 1: 使用默认配置（自动加载）
BasicWorkflow workflow = new BasicWorkflow();
workflow.runWorkflow();

// 方式 2: 传递自定义配置
Configuration config = FileSystemConfigLoader.createDefaultConfig();
workflow.runWorkflow(config);
```

#### KafkaStreamWorkflow

```java
// 方式 1: 使用默认配置（自动加载）
KafkaStreamWorkflow workflow = new KafkaStreamWorkflow();
workflow.runKafkaWorkflow("localhost:9092", "input-topic", "output-topic");

// 方式 2: 传递自定义配置
Configuration config = FileSystemConfigLoader.createDefaultConfig();
workflow.runKafkaWorkflow("localhost:9092", "input-topic", "output-topic", config);
```

## 🔍 配置加载器 API

### FileSystemConfigLoader 主要方法

```java
import com.example.project.base.util.FileSystemConfigLoader;
import org.apache.flink.configuration.Configuration;

// 方法 1: 创建默认配置
Configuration config = FileSystemConfigLoader.createDefaultConfig();

// 方法 2: 加载到现有配置
Configuration existingConfig = new Configuration();
Configuration loadedConfig = FileSystemConfigLoader.loadFileSystemConfig(existingConfig);
```

### 配置常量

```java
// MinIO 配置键
FileSystemConfigLoader.MINIO_ENDPOINT
FileSystemConfigLoader.MINIO_ACCESS_KEY
FileSystemConfigLoader.MINIO_SECRET_KEY
FileSystemConfigLoader.MINIO_USE_HTTPS

// S3A 配置键
FileSystemConfigLoader.S3A_ENDPOINT
FileSystemConfigLoader.S3A_ACCESS_KEY
FileSystemConfigLoader.S3A_SECRET_KEY
FileSystemConfigLoader.S3A_PATH_STYLE_ACCESS

// 状态后端配置键
FileSystemConfigLoader.STATE_CHECKPOINTS_DIR
FileSystemConfigLoader.STATE_SAVEPOINTS_DIR
```

## ✅ 配置验证

启动应用后，检查日志输出确认配置是否正确加载：

```
INFO  开始加载文件系统配置...
INFO  MinIO 配置 - Endpoint: http://minio:9000, UseHttps: false
INFO  S3A 文件系统配置完成
INFO  状态后端目录配置完成 - Checkpoints: s3a://flink-checkpoints/, Savepoints: s3a://flink-savepoints/
INFO  文件系统配置加载成功
```

## 🛠️ 故障排查

### 问题 1: 配置未生效

**解决方案：**
1. 检查 `application.properties` 文件是否存在且路径正确
2. 确认配置项名称拼写正确
3. 查看日志输出确认配置加载情况

### 问题 2: MinIO 连接失败

**解决方案：**
1. 确认 MinIO 服务已启动并可访问
2. 检查 endpoint 地址是否正确
3. 验证 access-key 和 secret-key 是否正确
4. 确认网络连通性

### 问题 3: S3A 文件系统无法访问

**解决方案：**
1. 确认 `hadoop-aws` 依赖已添加到 pom.xml
2. 检查 `fs.s3a.impl` 配置是否正确
3. 验证 S3A 相关配置参数

## 📦 依赖要求

确保 `flink-base/pom.xml` 中包含以下依赖：

```xml
<!-- Flink Core Dependencies -->
<dependency>
    <groupId>org.apache.flink</groupId>
    <artifactId>flink-core</artifactId>
</dependency>

<!-- Hadoop AWS for S3A support -->
<dependency>
    <groupId>org.apache.hadoop</groupId>
    <artifactId>hadoop-aws</artifactId>
    <version>3.3.4</version>
</dependency>

<!-- Flink Core Module (provides utility classes) -->
<dependency>
    <groupId>com.example.project</groupId>
    <artifactId>flinkcore</artifactId>
    <version>0.0.1-SNAPSHOT</version>
</dependency>
```

## 🎯 最佳实践

1. **统一配置管理**: 所有文件系统相关配置集中在 `application.properties` 中管理
2. **环境变量优先**: 生产环境使用环境变量覆盖默认配置
3. **配置验证**: 启动时使用 `config` 命令验证配置是否正确
4. **日志监控**: 关注启动日志中的配置加载信息
5. **错误处理**: 使用 try-catch 包裹配置加载代码以处理异常情况

## 📚 相关文档

- [Flink 官方文档 - 状态后端](https://nightlies.apache.org/flink/flink-docs-stable/docs/ops/state/state_backends/)
- [Hadoop S3A 文档](https://hadoop.apache.org/docs/current/hadoop-aws/tools/hadoop-aws/index.html)
- [MinIO 官方文档](https://min.io/docs/minio/linux/index.html)
