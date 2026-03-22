# Flink Core 集成 Elasticsearch 实现方案

## 实现概述

本方案实现了 Flink Base 模块通过 flink-core 提供的 ES 客户端，实时将数据写入独立部署的 Elasticsearch 服务。

## 核心组件

### 1. EsWriter (flink-core)

**位置**: `flink-core/src/main/java/com/example/project/core/es/EsWriter.java`

**功能**:
- 轻量级 ES 写入工具类
- 实现 `Serializable` 接口，支持 Flink 算子序列化
- 懒加载连接初始化，避免序列化问题
- 支持批量写入优化性能

**关键方法**:
```java
// 写入单个文档
void write(String indexName, String id, Object document)

// 批量写入文档
void bulkWrite(String indexName, List<Map<String, Object>> documents)

// 关闭客户端
void close()
```

### 2. EsSinkFunction (flink-core)

**位置**: `flink-core/src/main/java/com/example/project/core/es/EsSinkFunction.java`

**功能**:
- Flink Sink Function 实现（继承 RichSinkFunction）
- 内置批量缓冲和定时刷新机制
- 每个并行子任务独立维护 EsWriter 实例
- 自动管理生命周期（open/close）

**使用示例**:
```java
DataStream<Order> orders = ...;

orders.addSink(new EsSinkFunction<>(
    "http://es-pod:9200",  // ES 地址
    "orders",              // 索引名称
    500,                   // 批量大小
   3000L,                 // 刷新间隔 (ms)
    order -> {             // 映射函数
        Map<String, Object> doc = new HashMap<>();
        doc.put("orderId", order.getId());
        doc.put("amount", order.getAmount());
        return doc;
    }
));
```

### 3. EsStreamWorkflow (flink-base)

**位置**: `flink-base/src/main/java/com/example/project/base/workflow/EsStreamWorkflow.java`

**功能**:
- 演示如何在 Flink 作业中使用 ES Sink
- 从数据源读取数据并写入 ES
- 可被 FlinkBase 入口类调用

## 架构设计

### 模块依赖关系

```
flink-base (应用层)
    ↓ 依赖
flink-core (核心组件层)
    ↓ 使用
Elasticsearch Java Client
    ↓ 连接
Elasticsearch Service (独立部署)
```

### 运行时流程

```
1. FlinkBase 启动
   ↓
2. 加载 EsStreamWorkflow
   ↓
3. 创建 Flink 作业
   ↓
4. 添加 EsSinkFunction 到数据流
   ↓
5. Flink 序列化 EsSinkFunction 分发到 TaskManager
   ↓
6. TaskManager 反序列化，调用 open() 初始化 EsWriter
   ↓
7. 数据到达时 invoke() 方法缓冲数据
   ↓
8. 达到批量条件时 bulkWrite() 写入 ES
   ↓
9. 作业关闭时 flush 剩余数据并关闭连接
```

## 部署模式

### 模式 1: 本地开发

```bash
# 设置环境变量
export ES_HOSTS="http://localhost:9200"

# 运行工作流
cd flink-base
mvn spring-boot:run -Dspring-boot.run.arguments="es"
```

### 模式 2: Kubernetes Pod 部署

```yaml
apiVersion: apps/v1
kind: Deployment
metadata:
  name: flink-taskmanager
spec:
  template:
   spec:
     containers:
      - name: taskmanager
        image: flink-base:latest
       env:
        - name: ES_HOSTS
         value: "http://elasticsearch-service:9200"
```

### 模式 3: 独立服务器部署

```properties
# application.properties
es.hosts=http://es-server:9200
es.bulk-size=1000
es.flush-interval=5000
```

## 数据流示例

### 订单数据写入 ES

```java
// 1. 定义订单模型
public class Order {
    private String id;
    private Double amount;
    private String userId;
    private Long timestamp;
    // getters/setters
}

// 2. 创建数据流
DataStream<Order> orderStream = env.fromSource(kafkaSource, ...);

// 3. 写入 ES
orderStream.addSink(new EsSinkFunction<>(
    esHosts,
    "orders",
    order -> {
        Map<String, Object> doc = new HashMap<>();
        doc.put("orderId", order.getId());
        doc.put("amount", order.getAmount());
        doc.put("userId", order.getUserId());
        doc.put("timestamp", order.getTimestamp());
        return doc;
    }
));
```

## 配置参数

### 环境变量

| 变量名 | 说明 | 默认值 |
|--------|------|--------|
| ES_HOSTS | ES 服务地址 | 无（必填） |
| ES_BULK_SIZE | 批量大小 | 1000 |
| ES_FLUSH_INTERVAL | 刷新间隔 (ms) | 5000 |

### 构造函数参数

**EsSinkFunction**:
- `esHosts`: ES 地址（支持多节点逗号分隔）
- `indexName`: 索引名称
- `bulkSize`: 批量大小（默认 1000）
- `flushIntervalMs`: 刷新间隔（默认 5000ms）
- `mapper`: 数据映射函数

## 性能优化

### 1. 批量参数调优

```java
// 高吞吐场景
new EsSinkFunction<>(..., 5000, 2000L, ...)  // 大批量 + 短间隔

// 低延迟场景  
new EsSinkFunction<>(..., 200, 1000L, ...)   // 小批量 + 快刷新
```

### 2. 并行度设置

```java
env.setParallelism(4);  // 4 个并行实例同时写入
```

### 3. ES 索引优化

```bash
PUT /orders
{
  "settings": {
    "number_of_shards": 3,
    "refresh_interval": "5s"  // 降低刷新频率提高写入性能
  }
}
```

## 容错机制

### Checkpoint 配置

```java
// 启用 Checkpoint
env.enableCheckpointing(60000);

// 设置重启策略
env.setRestartStrategy(RestartStrategies.fixedDelayRestart(
    3,  // 重试次数
    Time.of(10, TimeUnit.SECONDS)  // 间隔
));
```

### 失败处理

- 网络异常自动重试（Flink 层面）
- 批量写入失败抛出异常触发重启
- close() 时保证缓冲区数据刷新

## 监控指标

### 关键指标

- 写入吞吐量（records/second）
- 批量写入延迟（ms）
- ES 响应时间
- 缓冲区大小

### 日志输出

```java
// INFO: 连接成功、批量写入成功
LOG.info("批量写入成功 - Index: {}, 数量：{}", indexName, documents.size());

// ERROR: 写入失败
LOG.error("写入文档失败：{}/{}", indexName, id, e);
```

## 使用示例代码

### 完整 Workflow

```java
public class EsStreamWorkflow {
    
  public void runEsWorkflow(String esHosts) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
       env.setParallelism(2);
        
        // 模拟数据源
        DataStream<Order> orders = env.fromSequence(1, 1000)
            .map(seq -> createOrder(seq));
        
        // 写入 ES
        orders.addSink(new EsSinkFunction<>(
            esHosts,
            "orders",
            500,
           3000L,
            order -> convertToDoc(order)
        ));
        
       env.execute("ES Workflow");
    }
    
    private Order createOrder(Long seq) {
        Order order= new Order();
        order.setId("order-" + seq);
        order.setAmount(Math.random() * 1000);
        return order;
    }
    
    private Map<String, Object> convertToDoc(Order order) {
        Map<String, Object> doc = new HashMap<>();
        doc.put("orderId", order.getId());
        doc.put("amount", order.getAmount());
        doc.put("timestamp", System.currentTimeMillis());
        return doc;
    }
}
```

## 与其他模块集成

### 从 Kafka 读取写入 ES

```java
// Kafka Source
KafkaSource<String> kafkaSource = KafkaSource.<String>builder()
    .setBootstrapServers("kafka:9092")
    .setTopics("orders")
    .build();

// 处理并写入 ES
env.fromSource(kafkaSource, WatermarkStrategy.noWatermarks(), "Kafka Source")
   .map(json -> parseOrder(json))
   .addSink(new EsSinkFunction<>(...));
```

### 结合 MinIO 使用

```java
// 从 MinIO 读取文件 → 处理 → 写入 ES
DataStream<String> lines = env.readFile(...);
lines.map(line -> extractData(line))
     .addSink(new EsSinkFunction<>(...));
```

## 常见问题

### Q: 如何处理 ES 地址动态配置？

A: 使用环境变量 + 参数优先级：
```java
String esHosts = System.getenv("ES_HOSTS");
if (esHosts == null) {
    esHosts = args[0];  // 命令行参数
}
```

### Q: 如何保证 Exactly-Once 语义？

A: 
1. 启用 Flink Checkpoint
2. 当前版本为 At-Least-Once
3. 需要时可扩展 TwoPhaseCommitSinkFunction

### Q: 批量写入失败如何处理？

A: 
- 抛出异常触发 Flink 重启
- 配合 Checkpoint 恢复状态
- 可添加死信队列处理失败记录

## 下一步改进

- [ ] 支持 TwoPhaseCommit 实现 Exactly-Once
- [ ] 添加异步批量写入
- [ ] 集成 Metrics 系统
- [ ] 支持动态索引分片（按时间）
- [ ] 添加背压感知机制

## 相关文件清单

### flink-core 模块
- `EsWriter.java` - ES 写入工具
- `EsSinkFunction.java` - Flink Sink 算子

### flink-base 模块
- `EsStreamWorkflow.java` - ES 工作流示例
- `FlinkBase.java` - 应用入口（已更新支持 es 命令）
- `pom.xml` - 添加 ES 依赖

### 配置文件
- `kubernetes-es-deployment.yaml` - K8s 部署示例
- `ELASTICSEARCH_INTEGRATION.md` - 集成文档

## 总结

本方案的核心优势：

1. **模块化设计**: flink-core 提供通用能力，flink-base 专注业务逻辑
2. **易于部署**: 支持环境变量配置，适配 K8s 环境
3. **高性能**: 批量缓冲 + 定时刷新机制
4. **易扩展**: 基于 Flink 生态，可轻松集成其他数据源
5. **生产就绪**: 包含容错、监控、日志等生产特性
