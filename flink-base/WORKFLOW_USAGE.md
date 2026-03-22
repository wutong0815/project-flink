# Flink Base 工作流使用指南

## 完整数据流水线架构

```
原始数据 → Kafka (入库) → Flink (实时处理) → Elasticsearch (存储/查询)
```

## 快速开始

### 1. 准备工作

#### 启动 Kafka
```bash
docker run -d --name kafka \
  -p 9092:9092 \
  -e KAFKA_BROKER_ID=1 \
  -e KAFKA_ZOOKEEPER_CONNECT=localhost:2181 \
  -e KAFKA_ADVERTISED_LISTENERS=PLAINTEXT://localhost:9092 \
  bitnami/kafka:latest
```

创建主题：
```bash
docker exec -it kafka kafka-topics.sh --create \
  --bootstrap-server localhost:9092 \
  --topic orders \
  --partitions 3 \
  --replication-factor 1
```

#### 启动 Elasticsearch
```bash
docker run -d --name elasticsearch \
  -p 9200:9200 \
  -e "discovery.type=single-node" \
  -e "xpack.security.enabled=false" \
  docker.elastic.co/elasticsearch/elasticsearch:8.11.0
```

### 2. 构建项目

```bash
cd flink-base
mvn clean package
```

生成的 jar 包位置：`target/flinkbase-0.0.1-SNAPSHOT.jar`

### 3. 运行工作流

#### 模式 1: 基础数据处理（DataGenerator 测试）
```bash
java -jar target/flinkbase-0.0.1-SNAPSHOT.jar basic
```

#### 模式 2: Kafka 流处理（Kafka → 处理 → Kafka）
```bash
java -jar target/flinkbase-0.0.1-SNAPSHOT.jar kafka \
  localhost:9092 \
  orders \
  order-aggregates
```

#### 模式 3: Elasticsearch 写入（数据源 → ES）
```bash
java -jar target/flinkbase-0.0.1-SNAPSHOT.jar es \
  http://localhost:9200
```

#### 模式 4: **完整流水线**（Kafka → Flink → ES）⭐
```bash
java -jar target/flinkbase-0.0.1-SNAPSHOT.jar pipeline \
  localhost:9092 \      # Kafka 地址
  orders \              # Kafka 主题
  http://localhost:9200 \  # ES 地址
  orders-index          # ES 索引
```

### 4. 提交到 Flink 集群

```bash
# 上传 jar 到 Flink
curl -X POST -F jarfile=@target/flinkbase-0.0.1-SNAPSHOT.jar \
  http://flink-jobmanager:8081/jars/upload

# 运行流水线作业
curl -X POST -H "Content-Type: application/json" \
  http://flink-jobmanager:8081/jobs/:jobid/run \
  -d '{
    "programArgs": "pipeline localhost:9092 orders http://es:9200 orders-index"
  }'
```

## 数据格式说明

### 输入数据（Kafka）

发送订单数据到 Kafka：
```json
{
  "orderId": "order-001",
  "userId": "user-123",
  "amount": 599.99,
  "status": "COMPLETED",
  "timestamp": 1710000000000
}
```

测试命令：
```bash
# 发送测试数据
echo '{"orderId":"order-001","userId":"user-123","amount":599.99,"status":"COMPLETED","timestamp":1710000000000}' | \
  kafka-console-producer.sh --bootstrap-server localhost:9092 --topic orders
```

### 输出数据（Elasticsearch）

处理后的数据结构：
```json
{
  "_index": "orders-index",
  "_id": "order-001",
  "_source": {
    "orderId": "order-001",
    "userId": "user-123",
    "amount": 599.99,
    "status": "COMPLETED",
    "orderTime": 1710000000000,
    "processTime": 1710000001234
  }
}
```

查询验证：
```bash
curl -X GET "http://localhost:9200/orders-index/_search?pretty"
```

## 工作流详解

### Pipeline 工作流（核心功能）

```java
┌─────────────┐
│   Kafka     │  原始订单数据
│  (orders)   │
└──────┬──────┘
       │
       ▼
┌─────────────────────────────────┐
│           Flink                 │
│  1. JSON 解析                   │
│  2. 过滤无效订单                │
│  3. 数据转换和格式化            │
│  4. 添加处理时间戳              │
└──────┬──────────────────────────┘
       │
       ▼
┌─────────────────┐
│ Elasticsearch   │  可查询的索引
│ (orders-index)  │
└─────────────────┘
```

### 数据处理逻辑

1. **JSON 解析**: 将 Kafka 中的 JSON 字符串解析为 Order 对象
2. **数据过滤**: 
   - 过滤金额 ≤ 0 的订单
   - 过滤状态为 CANCELLED 的订单
3. **数据转换**: 
   - 添加 `processTime`（处理时间戳）
   - 转换为 `OrderProcessed` 对象
4. **批量写入 ES**:
   - 每 500 条或 3 秒刷新一次
   - 使用 `orderId` 作为文档 ID（支持幂等更新）

## 配置参数

### 命令行参数

| 参数位置 | 含义 | 默认值 | 示例 |
|---------|------|--------|------|
| args[0] | 工作流类型 | basic | pipeline |
| args[1] | Kafka 地址 | localhost:9092 | kafka-cluster:9092 |
| args[2] | Kafka 主题 | orders | user-events |
| args[3] | ES 地址 | http://localhost:9200 | http://es-cluster:9200 |
| args[4] | ES 索引 | orders-index | orders-2024-03 |

### 环境变量

| 变量名 | 说明 | 优先级 |
|-------|------|--------|
| ES_HOSTS | ES 服务地址 | 高于命令行参数 |

```bash
export ES_HOSTS="http://production-es:9200"
java -jar flink-base.jar pipeline localhost:9092 orders
```

## 生产环境部署

### Kubernetes 部署

```yaml
apiVersion: apps/v1
kind: Deployment
metadata:
  name: flink-pipeline
spec:
  replicas: 2
  template:
    spec:
      containers:
      - name: flink-taskmanager
        image: your-registry/flink-base:latest
        command: ["java", "-jar", "flink-base.jar"]
        args: [
          "pipeline",
          "kafka-service:9092",
          "orders",
          "http://elasticsearch:9200",
          "orders-prod"
        ]
        env:
        - name: FLINK_JOBMANAGER_HOST
          value: "flink-jobmanager"
```

### Docker Compose 完整示例

```yaml
version: '3.8'
services:
  zookeeper:
    image: bitnami/zookeeper:latest
    environment:
      - ALLOW_ANONYMOUS_LOGIN=yes
  
  kafka:
    image: bitnami/kafka:latest
    depends_on:
      - zookeeper
    ports:
      - "9092:9092"
    environment:
      - KAFKA_BROKER_ID=1
      - KAFKA_ZOOKEEPER_CONNECT=zookeeper:2181
      - KAFKA_ADVERTISED_LISTENERS=PLAINTEXT://kafka:9092
  
  elasticsearch:
    image: docker.elastic.co/elasticsearch/elasticsearch:8.11.0
    environment:
      - discovery.type=single-node
      - xpack.security.enabled=false
  
  flink-jobmanager:
    image: your-registry/flink-base:latest
    command: jobmanager
    ports:
      - "8081:8081"
    depends_on:
      - kafka
      - elasticsearch
  
  flink-taskmanager:
    image: your-registry/flink-base:latest
    command: taskmanager
    depends_on:
      - flink-jobmanager
      - kafka
      - elasticsearch
```

启动：
```bash
docker-compose up -d
```

## 监控与调试

### 查看 Flink Web UI
```
http://localhost:8081
```

### 查看处理日志
```bash
docker logs flink-taskmanager | grep "处理结果"
```

### 验证 ES 数据
```bash
# 统计文档数量
curl http://localhost:9200/orders-index/_count

# 搜索特定用户订单
curl -X GET "http://localhost:9200/orders-index/_search?q=userId:user-123"
```

## 常见问题

### Q: 如何停止作业？
```bash
# 找到作业 ID
curl http://localhost:8081/jobs

# 取消作业
curl -X PATCH http://localhost:8081/jobs/:jobid/cancel
```

### Q: 如何处理大量数据？
调整并行度和批量参数：
```java
env.setParallelism(4);  // 增加并行度

new EsSinkFunction<>(
    esHosts,
    esIndex,
    2000,    // 增大批量
   2000L,   // 缩短间隔
    mapper
);
```

### Q: 数据丢失怎么办？
启用 Checkpoint：
```java
env.enableCheckpointing(60000);  // 每分钟 checkpoint
env.getCheckpointConfig().setMinPauseBetweenCheckpoints(30000);
```

## 性能优化建议

1. **增加并行度**: 根据数据量设置合适的 parallelism
2. **调整批量大小**: 高吞吐场景增大批量，低延迟场景减小批量
3. **ES 索引优化**: 设置合适的分片数和副本数
4. **网络优化**: 确保 Flink、Kafka、ES 在同一网络区域

## 下一步扩展

- [ ] 添加多个数据源支持
- [ ] 实现窗口聚合功能
- [ ] 集成监控系统（Prometheus + Grafana）
- [ ] 添加死信队列处理失败数据
