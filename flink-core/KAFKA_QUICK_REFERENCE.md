# Kafka 客户端快速参考

## 核心 API

### 创建 Kafka 客户端

```java
// 基本用法
KafkaClientUtil kafkaClient = new KafkaClientUtil("localhost:9092", "my-group");

// 自定义 offset 重置策略（false 表示从最新 offset 开始）
KafkaClientUtil kafkaClient = new KafkaClientUtil("localhost:9092", "my-group", false);
```

### 创建 Source (消费者)

```java
// 单主题
KafkaSource<String> source = kafkaClient.createSource("input-topic");

// 多主题
KafkaSource<String> multiSource = kafkaClient.createSource("topic-1", "topic-2", "topic-3");
```

### 创建 Sink (生产者)

```java
// 默认字符串序列化
KafkaSink<String> sink = kafkaClient.createSink("output-topic");

// 自定义序列化器
KafkaSink<MyObject> sink = kafkaClient.createSink(
    "output-topic", 
    new MySerializationSchema()
);
```

## 在 Flink 作业中使用

```java
StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

// 初始化客户端
KafkaClientUtil kafkaClient = new KafkaClientUtil("localhost:9092", "flink-job-group");

// 创建 Source 和 Sink
var source = kafkaClient.createSource("input");
var sink = kafkaClient.createSink("output");

// 构建数据流
DataStream<String> stream = env.fromSource(
    source, 
    WatermarkStrategy.noWatermarks(), 
    "Kafka Input"
);

// 处理并写入
stream
    .map(data -> process(data))
    .sinkTo(sink);

// 执行
env.execute("My Kafka Job");
```

## 命令行测试

### 运行示例
```bash
cd flink-core
mvn spring-boot:run -Dspring-boot.run.arguments="kafka-example"
```

### 快速测试
```bash
mvn spring-boot:run -Dspring-boot.run.arguments="kafka localhost:9092 my-group my-topic"
```

## 常用配置参数

| 参数 | 说明 | 示例值 |
|------|------|--------|
| bootstrapServers | Kafka 服务器地址 | localhost:9092 |
| groupId | 消费者组 ID | my-application-group |
| autoOffsetResetEarliest | 是否从最早 offset 消费 | true/false |

## 完整工作流示例

```java
import com.example.project.core.kafka.KafkaClientUtil;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;

public class KafkaProcessingWorkflow {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        
        // 1. 初始化 Kafka 客户端
        KafkaClientUtil kafkaClient = new KafkaClientUtil(
            "localhost:9092", 
            "order-processing-group"
        );
        
        // 2. 验证配置
        if (!kafkaClient.isValidConfig()) {
            throw new RuntimeException("Kafka 配置无效");
        }
        
        // 3. 创建 Source 和 Sink
        var source = kafkaClient.createSource("orders");
        var sink = kafkaClient.createSink("processed-orders");
        
        // 4. 构建处理流程
        DataStream<String> orderStream = env.fromSource(
            source,
            WatermarkStrategy.noWatermarks(),
            "Orders Input"
        );
        
        // 5. 数据处理
        orderStream
            .filter(order -> order.contains("valid"))
            .map(order -> {
                // 处理订单逻辑
                return "Processed: " + order;
            })
            .addSink((sink) -> {
                // 自定义 sink 逻辑（可选）
            })
            .sinkTo(sink);
        
        // 6. 执行作业
        env.execute("Order Processing Workflow");
    }
    
    private static String process(String order) {
        // 业务处理逻辑
        return order;
    }
}
```

## 错误处理建议

```java
try {
    KafkaClientUtil kafkaClient = new KafkaClientUtil(servers, groupId);
    
    if (!kafkaClient.isValidConfig()) {
        LOG.error("Kafka 配置验证失败");
        return;
    }
    
    var source = kafkaClient.createSource(topic);
    var sink = kafkaClient.createSink(topic);
    
    // 使用 Source 和 Sink
    
} catch (Exception e) {
    LOG.error("Kafka 客户端操作失败", e);
    throw new RuntimeException("Kafka 操作失败", e);
}
```

## 最佳实践

1. **复用客户端实例**: 在一个作业中尽量复用同一个 KafkaClientUtil 实例
2. **合理设置 Group ID**: 确保每个独立的消费组有唯一的 Group ID
3. **配置检查**: 在创建 Source/Sink 前先调用 isValidConfig() 验证配置
4. **异常处理**: 捕获并记录所有 Kafka 相关异常
5. **资源管理**: 虽然 KafkaClientUtil 不需要显式关闭，但应该在作业结束时释放引用

## 常见问题排查

### 连接失败
```bash
# 检查 Kafka 是否运行
docker ps | grep kafka

# 测试网络连通性
telnet localhost 9092
```

### Topic 不存在
```bash
# 列出所有 topic
kafka-topics.sh --bootstrap-server localhost:9092 --list

# 创建 topic
kafka-topics.sh --bootstrap-server localhost:9092 --create --topic my-topic --partitions 3 --replication-factor 1
```

### 消费组问题
```bash
# 查看消费组列表
kafka-consumer-groups.sh --bootstrap-server localhost:9092 --list

# 查看消费组详情
kafka-consumer-groups.sh --bootstrap-server localhost:9092 --describe --group my-group
```
