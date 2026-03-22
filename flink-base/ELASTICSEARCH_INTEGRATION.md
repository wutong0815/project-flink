# Elasticsearch 集成方案

## 架构设计

### 模块职责

**flink-core 模块：**
- `EsWriter` - 轻量级 ES 写入工具，实现 Serializable，支持在 Flink 算子中序列化传输
- `EsSinkFunction` - Flink Sink 算子，封装批量缓冲和定时刷新逻辑

**flink-base 模块：**
- `EsStreamWorkflow` - 示例工作流，演示如何在 Flink 作业中使用 ES Sink
- 依赖 flink-core 的 jar 包，运行时调用 EsWriter 和 EsSinkFunction

### 部署架构

```
┌─────────────────────────────────────┐
│  Kubernetes Cluster                 │
│                                     │
│  ┌─────────────────────────────┐   │
│  │  Flink JobManager Pod       │   │
│  │  (运行 flink-base)          │   │
│  │                             │   │
│  │  Flink 作业启动时           │   │
│  │  → 加载 flink-core.jar      │   │
│  │  → 创建 EsSinkFunction      │   │
│  │  → 初始化 EsWriter          │   │
│  └─────────────────────────────┘   │
│                                     │
│  ┌─────────────────────────────┐   │
│  │  Flink TaskManager Pod      │   │
│  │  (执行数据写入)             │   │
│  │                             │   │
│  │  EsSinkFunction.invoke()    │   │
│  │  → 缓冲数据                 │   │
│  │  → 批量写入 ES              │   │
│  └─────────────────────────────┘   │
│                                     │
└─────────────────────────────────────┘
                  │
                  │ HTTP/REST API
                  ▼
┌─────────────────────────────────────┐
│  Elasticsearch Service (独立部署)   │
│                                     │
│  ┌─────────────────────────────┐   │
│  │  ES Pod 1                   │   │
│  │  - 数据存储                 │   │
│  │  - 索引管理                 │   │
│  └─────────────────────────────┘   │
│                                     │
│  ┌─────────────────────────────┐   │
│  │  ES Pod 2                   │   │
│  │  - 数据副本                 │   │
│  │  - 负载均衡                 │   │
│  └─────────────────────────────┘   │
└─────────────────────────────────────┘
```

## 使用方式

### 1. 本地开发测试

```bash
# 设置环境变量（可选）
export ES_HOSTS="http://localhost:9200"

# 运行 ES 工作流
cd flink-base
mvn spring-boot:run -Dspring-boot.run.arguments="es"

# 或者通过参数指定 ES 地址
mvn spring-boot:run -Dspring-boot.run.arguments="es http://localhost:9200"
```

### 2. Kubernetes 部署

#### 配置环境变量

在 Flink Pod 的部署文件中添加环境变量：

```yaml
apiVersion: apps/v1
kind: Deployment
metadata:
  name: flink-jobmanager
spec:
  template:
    spec:
      containers:
      - name: flink-jobmanager
        image: your-registry/flink-base:latest
        env:
        - name: ES_HOSTS
          value: "http://elasticsearch-service:9200"
        # 其他 Flink 配置...
```

#### Service 发现

如果 ES 以 StatefulSet 部署在同一集群：

```yaml
# elasticsearch-service.yaml
apiVersion: v1
kind: Service
metadata:
  name: elasticsearch-service
spec:
  selector:
    app: elasticsearch
  ports:
  - port: 9200
    targetPort: 9200
  clusterIP: None  # Headless Service
```

Flink 中直接使用：
```java
String esHosts = System.getenv("ES_HOSTS"); 
// 或 "http://elasticsearch-service:9200"
```

### 3. 自定义工作流

在 flink-base 中创建自己的 Workflow：

```java
public class MyCustomWorkflow {
    
   public void runWorkflow(String esHosts) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        
        // 从 Kafka 读取数据
        DataStream<Order> orders = env.addSource(
            KafkaSource.<String>builder()
                .setBootstrapServers("kafka:9092")
                .setTopics("orders")
                .build()
        );
        
        // 处理并写入 ES
        orders.map(order -> parseOrder(order))
              .addSink(new EsSinkFunction<>(
                  esHosts,
                  "orders-index",
                  1000,     // bulk size
                  5000L,    // flush interval
                  order -> {
                      Map<String, Object> doc = new HashMap<>();
                      doc.put("orderId", order.getId());
                      doc.put("amount", order.getAmount());
                      return doc;
                  }
              ));
        
        env.execute("My Custom Workflow");
    }
}
```

## 核心代码示例

### EsWriter 使用

```java
// 直接调用（不推荐用于 Flink 作业）
EsWriter writer = new EsWriter("http://es:9200", 1000, 5000L);

try {
    Map<String, Object> doc = new HashMap<>();
    doc.put("field", "value");
    
    writer.write("my-index", "doc-id", doc);
    
} finally {
    writer.close();
}
```

### EsSinkFunction 在 Flink 中使用

```java
DataStream<MyData> stream = ...;

stream.addSink(new EsSinkFunction<>(
    "http://es-pod:9200",  // ES 地址
    "my-index",            // 索引名
   1000,                  // 批量大小
    5000L,                 // 刷新间隔 (ms)
    data -> {              // 映射函数
        Map<String, Object> doc = new HashMap<>();
        doc.put("id", data.getId());
        doc.put("value", data.getValue());
        doc.put("timestamp", System.currentTimeMillis());
        return doc;
    }
));
```

## 配置参数说明

### EsWriter 构造参数

| 参数 | 类型 | 默认值 | 说明 |
|------|------|--------|------|
| hosts | String | 必填 | ES 服务器地址，支持多节点（逗号分隔） |
| username | String | null | 用户名（可选） |
| password | String | null | 密码（可选） |
| bulkSize | int | 1000 | 批量写入条数 |
| flushIntervalMs | long | 5000 | 刷新间隔（毫秒） |

### EsSinkFunction 构造参数

| 参数 | 类型 | 默认值 | 说明 |
|------|------|--------|------|
| esHosts | String | 必填 | ES 服务器地址 |
| indexName | String | 必填 | 索引名称 |
| bulkSize | int | 1000 | 批量大小 |
| flushIntervalMs | long | 5000 | 刷新间隔 |
| mapper | DocumentMapper | 必填 | 数据映射函数 |

## 性能优化建议

### 1. 批量参数调优

```java
// 高吞吐场景（每秒万级数据）
new EsSinkFunction<>(
    esHosts,
    "index",
    5000,   // 增大批量大小
   2000L,  // 缩短刷新间隔
    mapper
);

// 低延迟场景（秒级可见）
new EsSinkFunction<>(
    esHosts,
    "index",
   200,    // 减小批量大小
    1000L,  // 快速刷新
    mapper
);
```

### 2. Flink 并行度

```java
// 增加并行度以提高写入吞吐
env.setParallelism(4);

// 每个并行子任务独立维护一个 EsWriter 实例
// 总共有 parallelism 个连接写入 ES
```

### 3. ES 索引配置

```bash
# 创建索引时优化写入性能
PUT /orders
{
  "settings": {
    "number_of_shards": 3,      // 分片数
    "number_of_replicas": 1,    // 副本数
    "refresh_interval": "5s"    // 刷新间隔（降低频率提高写入性能）
  }
}
```

### 4. 网络优化

- 确保 Flink Pod 和 ES Pod 在同一网络区域
- 使用内网地址访问 ES
- 考虑使用连接池（已在 EsWriter 中内置）

## 容错与 Exactly-Once 语义

### Checkpoint 配置

```java
// 启用 Checkpoint 以保证 Exactly-Once
env.enableCheckpointing(60000); // 每分钟 checkpoint

// 配合 TwoPhaseCommitSinkFunction 可实现端到端 Exactly-Once
// （当前版本为 At-Least-Once，需要时可升级为 TwoPhaseCommit）
```

### 失败重试

```java
// Flink 自动重启策略
env.setRestartStrategy(RestartStrategies.fixedDelayRestart(
    3,  // 重试次数
    Time.of(10, TimeUnit.SECONDS)  // 重试间隔
));
```

## 监控与日志

### 关键指标监控

- 写入吞吐量（records/sec）
- 批量写入延迟
- ES 响应时间
- 缓冲区大小

### 日志输出

```java
// INFO: 连接成功、批量写入成功
// WARN: 空文档跳过
// ERROR: 写入失败、连接失败
```

## 常见问题

### Q: ES 地址如何配置？

A: 优先使用环境变量 `ES_HOSTS`，其次使用命令行参数，最后使用默认值。

```bash
# 方式 1: 环境变量
export ES_HOSTS="http://es-service:9200"

# 方式 2: 命令行参数
mvn spring-boot:run -Dspring-boot.run.arguments="es http://es-service:9200"
```

### Q: 如何处理认证？

A: 使用带认证的构造函数：

```java
new EsSinkFunction<>(
    "http://es:9200",
    "elastic",  // username
    "password", // password
    "index",
    1000,
    5000L,
    mapper
);
```

### Q: 如何保证数据不丢失？

A: 
1. 启用 Flink Checkpoint
2. 在 close() 时刷新缓冲区（已实现）
3. 配置合适的重试策略

### Q: 写入性能如何优化？

A:
1. 增大批量大小（bulkSize）
2. 调整刷新间隔（flushIntervalMs）
3. 增加 Flink 并行度
4. 优化 ES 索引配置（减少 refresh 频率）

## 下一步扩展

- [ ] 支持 TwoPhaseCommitSinkFunction 实现 Exactly-Once
- [ ] 添加异步批量写入支持
- [ ] 集成 Flink Metrics 系统监控性能指标
- [ ] 支持动态索引名称（按时间分片）
