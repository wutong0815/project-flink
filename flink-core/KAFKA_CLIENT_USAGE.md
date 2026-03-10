# Flink Core Kafka 客户端使用指南

## 概述

`flink-core` 模块提供了统一的 Kafka 客户端工具类 `KafkaClientUtil`,可供 `flink-base` 等其他模块调用。

## 功能特性

- **简化配置**: 统一管理 Kafka 连接配置（Bootstrap Servers、Group ID 等）
- **快速创建 Source/Sink**: 提供便捷方法创建 Kafka Source（消费者）和 Sink（生产者）
- **支持多主题**: 支持单主题和多主题的 Source 创建
- **自定义序列化**: 支持自定义序列化器的 Sink 创建

## 使用方法

### 1. 在 flink-base 中使用 Kafka 客户端

```java
import com.example.project.core.kafka.KafkaClientUtil;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.sink.KafkaSink;

// 创建 Kafka 客户端
String kafkaServers = "localhost:9092";
String groupId = "my-workflow-group";
KafkaClientUtil kafkaClient = new KafkaClientUtil(kafkaServers, groupId);

// 创建 Kafka Source（从单个主题读取）
KafkaSource<String> source = kafkaClient.createSource("input-topic");

// 或者创建多主题 Source
KafkaSource<String> multiTopicSource = kafkaClient.createSource("topic-1", "topic-2");

// 创建 Kafka Sink（写入到主题）
KafkaSink<String> sink = kafkaClient.createSink("output-topic");

// 在 Flink 作业中使用
DataStream<String> stream = env.fromSource(source, WatermarkStrategy.noWatermarks(), "Kafka Source");
stream.sinkTo(sink);
```

### 2. 通过命令行运行 FlinkCore

#### 运行 Kafka 示例
```bash
cd /Users/wutong/Public/ideaproject/IndependentProject/project-flink/flink-core
mvn spring-boot:run -Dspring-boot.run.arguments="kafka-example"
```

#### 初始化 Kafka 客户端并测试
```bash
# 使用默认配置 (localhost:9092, flink-core-group)
mvn spring-boot:run -Dspring-boot.run.arguments="kafka"

# 自定义配置
mvn spring-boot:run -Dspring-boot.run.arguments="kafka localhost:9092 my-group test-topic"
```

### 3. 在 FlinkBase 中使用

`FlinkBase` 模块的 `KafkaStreamWorkflow` 已经集成了 `KafkaClientUtil`:

```bash
cd /Users/wutong/Public/ideaproject/IndependentProject/project-flink/flink-base
mvn spring-boot:run -Dspring-boot.run.arguments="kafka localhost:9092 orders order-aggregates"
```

## API 参考

### KafkaClientUtil 构造函数

```java
// 使用默认配置（从最早 offset 开始消费）
public KafkaClientUtil(String bootstrapServers, String groupId)

// 自定义 offset 重置策略
public KafkaClientUtil(String bootstrapServers, String groupId, boolean autoOffsetResetEarliest)
```

### 主要方法

#### 创建 Source
```java
// 单主题
public KafkaSource<String> createSource(String topic)

// 多主题
public KafkaSource<String> createSource(String... topics)
```

#### 创建 Sink
```java
// 默认字符串序列化
public KafkaSink<String> createSink(String topic)

// 自定义序列化器
public <T> KafkaSink<T> createSink(String topic, SerializationSchema<T> serializationSchema)
```

#### 配置验证
```java
public boolean isValidConfig()
```

## 完整示例

### 实时数据处理工作流

```java
import com.example.project.core.kafka.KafkaClientUtil;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.datastream.DataStream;

public class RealTimeProcessing {
    public static void main(String[] args) throws Exception {
        // 创建执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        
        // 初始化 Kafka 客户端
        KafkaClientUtil kafkaClient = new KafkaClientUtil("localhost:9092", "realtime-processing-group");
        
        // 创建 Source 和 Sink
        var source = kafkaClient.createSource("user-events");
        var sink = kafkaClient.createSink("processed-events");
        
        // 构建处理流程
        DataStream<String> eventStream = env.fromSource(source, 
                WatermarkStrategy.noWatermarks(), "User Events");
        
        eventStream
            .filter(event -> event.contains("important"))
            .map(event -> processEvent(event))
            .sinkTo(sink);
        
        // 执行作业
        env.execute("Real-time Event Processing");
    }
    
    private static String processEvent(String event) {
        // 事件处理逻辑
        return "Processed: " + event;
    }
}
```

## 依赖配置

确保在 `pom.xml` 中包含以下依赖：

```xml
<!-- flink-core 模块 -->
<dependency>
    <groupId>com.example.project</groupId>
    <artifactId>flinkcore</artifactId>
    <version>0.0.1-SNAPSHOT</version>
</dependency>

<!-- Flink Kafka Connector (已在父 POM 中管理) -->
<dependency>
    <groupId>org.apache.flink</groupId>
    <artifactId>flink-connector-kafka</artifactId>
</dependency>
```

## 注意事项

1. **Kafka 集群**: 确保 Kafka 集群可访问
2. **Topic 预创建**: Kafka Topic 需要提前创建好
3. **网络配置**: 如果在容器或分布式环境中，注意网络可达性
4. **序列化器选择**: 根据业务数据选择合适的序列化方式

## 故障排查

### 常见问题

1. **连接失败**: 检查 Kafka 服务器地址和网络连通性
2. **认证错误**: 确认 Group ID 配置正确
3. **Topic 不存在**: 确保 Topic 已创建且名称正确

### 日志级别

可以通过调整日志级别来获取更多调试信息：

```properties
logging.level.com.example.project.core.kafka=DEBUG
```

## 架构说明

```
flink-core (提供 Kafka 客户端)
    └── KafkaClientUtil
    └── KafkaExample
    
flink-base (使用 Kafka 客户端)
    └── KafkaStreamWorkflow (通过 KafkaClientUtil 创建 Source/Sink)
```

这种设计使得 Kafka 相关的配置和创建逻辑集中在 `flink-core` 模块中，便于维护和复用。
