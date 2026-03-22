# 完整业务工作流使用指南

## 📋 概述

本工作流实现了完整的业务数据处理流程：**数据库 → Kafka → Flink(ES 增强) → 数据库**

### 业务流程

```
┌─────────────┐      ┌──────────┐      ┌─────────────────┐      ┌─────────────┐
│   数据库     │ ───► │  Kafka   │ ───► │  Flink 流处理    │ ───► │   数据库     │
│ (原始数据)   │      │ (消息队列)│      │ (ES 数据增强)    │      │ (历史数据)   │
└─────────────┘      └──────────┘      └─────────────────┘      └─────────────┘
     ▲                                                                        │
     │                                                                        │
     └────────────────────────────────────────────────────────────────────────┘
                                   (数据归档)
```

### 核心功能

1. **数据采集**: 从数据库读取原始订单数据
2. **消息队列**: 通过 Kafka 进行数据缓冲和解耦
3. **实时处理**: Flink 流处理引擎进行实时计算
4. **数据增强**: 查询 ES 获取用户画像、风控信息等
5. **规则过滤**: 根据业务规则过滤和转换数据
6. **历史归档**: 将处理结果写入数据库作为历史数据

---

## 🚀 快速开始

### 1. 环境准备

确保以下服务已启动:

- **MySQL/PostgreSQL** 数据库
- **Kafka** 消息队列
- **Elasticsearch** 搜索引擎

### 2. 数据库表结构

#### 原始订单表 (orders)

```sql
CREATE TABLE orders (
    order_id VARCHAR(64) PRIMARY KEY,
    user_id VARCHAR(64) NOT NULL,
    amount DECIMAL(10,2) NOT NULL,
    product VARCHAR(255),
    status VARCHAR(32),
    create_time BIGINT,
    INDEX idx_user_id (user_id),
    INDEX idx_status (status)
);
```

#### 历史数据表 (order_history)

```sql
CREATE TABLE order_history (
    id BIGINT AUTO_INCREMENT PRIMARY KEY,
    user_id VARCHAR(64) NOT NULL,
    total_amount DECIMAL(10,2) NOT NULL,
    process_time BIGINT NOT NULL,
    status VARCHAR(32),
    INDEX idx_user_id (user_id),
    INDEX idx_process_time (process_time)
);
```

### 3. 准备测试数据

```sql
INSERT INTO orders (order_id, user_id, amount, product, status, create_time) VALUES
('order-001', 'user-1', 199.99, 'Product A', 'PENDING', UNIX_TIMESTAMP() * 1000),
('order-002', 'user-2', 299.99, 'Product B', 'PENDING', UNIX_TIMESTAMP() * 1000),
('order-003', 'user-1', 99.99, 'Product C', 'PENDING', UNIX_TIMESTAMP() * 1000),
('order-004', 'user-3', 1500.00, 'Product D', 'PENDING', UNIX_TIMESTAMP() * 1000),
('order-005', 'user-2', 50.00, 'Product E', 'PENDING', UNIX_TIMESTAMP() * 1000);
```

---

## 💻 使用方法

### 方式一：命令行参数

```bash
# 运行完整业务工作流
java -jar flinkbase.jar business \
    "business-orders" \                    # 输入 Topic
    "business-results" \                   # 输出 Topic
    "jdbc:mysql://localhost:3306/flink_db?useSSL=false&serverTimezone=UTC" \
    "root" \                               # 数据库用户名
    "password" \                           # 数据库密码
    "localhost:9092" \                     # Kafka 地址
    "http://localhost:9200"                # ES 地址
```

### 方式二：环境变量 (推荐)

```bash
# 设置环境变量
export DB_URL="jdbc:mysql://localhost:3306/flink_db?useSSL=false&serverTimezone=UTC"
export DB_USER="root"
export DB_PASSWORD="password"
export KAFKA_SERVERS="localhost:9092"
export ES_HOSTS="http://localhost:9200"

# 运行工作流
java -jar flinkbase.jar business
```

### 方式三：Docker 容器化部署

```bash
# 使用 docker-compose 启动所有服务
docker-compose up -d

# 查看日志
docker-compose logs -f flink-base
```

---

## 🔧 配置说明

### 命令行参数详解

```bash
java -jar flinkbase.jar business \
    [inputTopic]        # 可选，默认：business-orders
    [outputTopic]       # 可选，默认：business-results
    [dbUrl]            # 可选，默认：jdbc:mysql://localhost:3306/flink_db
    [dbUser]           # 可选，默认：root
    [dbPassword]       # 可选，默认：password
    [kafkaServers]     # 可选，默认：localhost:9092
    [esHosts]          # 可选，默认：http://localhost:9200
```

### 环境变量优先级

环境变量优先于命令行参数:

1. `DB_URL` - 数据库 JDBC URL
2. `DB_USER` - 数据库用户名
3. `DB_PASSWORD` - 数据库密码
4. `KAFKA_SERVERS` - Kafka 服务器地址
5. `ES_HOSTS` - Elasticsearch 地址

---

## 📊 处理流程详解

### 阶段 1: 数据库 → Kafka

```java
// 从数据库读取 PENDING 状态的订单
List<Map<String, Object>> orders = jdbcClient.query(
    "SELECT * FROM orders WHERE status = ?", 
    "PENDING"
);

// 转换为 JSON 并发送到 Kafka
DataStream<String> orderStream = env.fromCollection(orders)
    .map(order -> MAPPER.writeValueAsString(order));

orderStream.sinkTo(kafkaSink);
```

### 阶段 2: Kafka → Flink 处理

```java
// 从 Kafka 消费数据
DataStream<String> kafkaStream = env.fromSource(
    kafkaSource,
    WatermarkStrategy.forBoundedOutOfOrderness(Duration.ofSeconds(5)),
    "Kafka Source"
);

// 处理流程
kafkaStream
    // 1. 解析 JSON
    .map(json -> parseOrder(json))
    
    // 2. 过滤无效订单
    .filter(order -> order.amount > 0)
    
    // 3. ES 数据增强 (示例中为模拟)
    .map(order -> Tuple2.of(order, getUserInfoFromES(order.userId)))
    
    // 4. 业务规则过滤
    .filter(tuple -> {
        UserInfo info = tuple.f1;
        return "VIP".equals(info.userLevel) || 
               (tuple.f0.amount < 500 && info.riskScore < 80);
    })
    
    // 5. 聚合统计
    .keyBy(value -> value.f0.userId)
    .sum(1);
```

### 阶段 3: 结果 → 数据库

```java
resultStream.addSink(new RichSinkFunction<OrderResult>() {
    private Connection conn;
    private PreparedStatement pstmt;
    
    @Override
    public void open(Configuration parameters) throws Exception {
        conn = jdbcClient.getConnection();
        pstmt = conn.prepareStatement(
            "INSERT INTO order_history (user_id, total_amount, process_time, status) VALUES (?, ?, ?, ?)"
        );
    }
    
    @Override
    public void invoke(OrderResult result, Context context) throws Exception {
        pstmt.setString(1, result.userId);
        pstmt.setDouble(2, result.totalAmount);
        pstmt.setLong(3, result.processTime);
        pstmt.setString(4, result.status);
        pstmt.executeUpdate();
    }
});
```

---

## 🎯 业务规则配置

### 当前规则

1. **VIP 用户**: 订单金额 > 1000，直接通过
2. **普通用户**: 
   - 订单金额 < 500 且风险评分 < 80，通过
   - 否则，过滤

### 自定义规则

修改 `BusinessDataWorkflow.java` 中的过滤逻辑:

```java
.filter(tuple -> {
    Order order = tuple.f0;
    UserInfo userInfo = tuple.f1;
    
    // 自定义业务规则
    if ("VIP".equals(userInfo.userLevel)) {
        return true;
    }
    
    // 高风险订单过滤
    if (userInfo.riskScore >= 80) {
        LOG.warn("高风险订单被过滤：userId={}, riskScore={}", 
                 order.userId, userInfo.riskScore);
        return false;
    }
    
    return order.amount < 500;
})
```

---

## 🔍 监控与调试

### 查看 Flink Web UI

访问：http://localhost:8081

- 查看作业状态
- 监控数据流量
- 检查反压情况

### 日志级别调整

```properties
# application.properties
logging.level.com.example.project.base.workflow=DEBUG
logging.level.com.example.project.core.jdbc=DEBUG
logging.level.com.example.project.core.kafka=DEBUG
logging.level.com.example.project.core.es=DEBUG
```

### 常见问题排查

#### 1. 数据库连接失败

```bash
# 检查数据库是否可访问
mysql -h localhost -u root -p

# 检查 JDBC URL 格式
echo $DB_URL
# 应该类似：jdbc:mysql://localhost:3306/flink_db?useSSL=false&serverTimezone=UTC
```

#### 2. Kafka Topic 不存在

```bash
# 创建 Topic
kafka-topics.sh --bootstrap-server localhost:9092 \
    --create --topic business-orders \
    --partitions 3 --replication-factor 1
```

#### 3. ES 连接失败

```bash
# 检查 ES 服务
curl http://localhost:9200

# 应该返回类似:
# {
#   "name" : "node-1",
#   "cluster_name" : "docker-cluster",
#   "version" : { ... }
# }
```

---

## 📈 性能优化建议

### 1. 并行度设置

```java
env.setParallelism(4);  // 根据 CPU 核心数调整
```

### 2. 检查点配置

```java
env.enableCheckpointing(5000);  // 5 秒一次检查点
env.getCheckpointConfig().setMinPauseBetweenCheckpoints(1000);
```

### 3. Kafka 批量发送

```java
// 在 KafkaProducer 配置中
props.setProperty("batch.size", "16384");
props.setProperty("linger.ms", "100");
```

### 4. 数据库批量插入

已在 `JdbcClientUtil` 中实现批量插入，每 100 条提交一次。

---

## 🛠️ 扩展功能

### 添加新的数据源

实现新的 Source 类:

```java
public class MyCustomSource implements SourceReader<MyRecord> {
    // 实现自定义数据源
}
```

### 集成 Redis 缓存

```java
// 在数据增强阶段使用 Redis 缓存用户信息
RedisClient redisClient = new RedisClient("localhost", 6379);
String userInfo = redisClient.get("user:" + order.userId);
```

### 添加实时监控指标

```java
// 使用 Flink Metrics 系统
MetricGroup metrics = getRuntimeContext().getMetricGroup();
Counter successCounter = metrics.counter("success_count");
Gauge<Long> latencyGauge = () -> System.currentTimeMillis() - eventTime;
```

---

## 📝 示例输出

### 控制台日志

```
========================================
启动完整业务工作流 (数据库→Kafka→Flink(ES)→数据库)...
========================================
数据库：jdbc:mysql://localhost:3306/flink_db
Kafka: localhost:9092
Elasticsearch: http://localhost:9200
输入 Topic: business-orders, 输出 Topic: business-results

阶段 1: 从数据库读取数据并发送到 Kafka...
从数据库读取到 5 条订单数据
数据已成功发送到 Kafka Topic: business-orders

阶段 2: 从 Kafka 消费数据并进行流处理...
解析订单：Order{orderId='order-001', userId='user-1', amount=199.99}
处理结果：OrderResult{userId='user-1', totalAmount=299.98, processTime=1234567890000, status='PROCESSED'}

阶段 3: 将处理结果写入数据库...
数据库连接已建立，准备写入数据

========================================
开始执行 Flink 作业...
========================================
```

### 数据库查询结果

```sql
SELECT * FROM order_history ORDER BY process_time DESC LIMIT 10;
```

返回:

| id | user_id | total_amount | process_time | status |
|----|---------|--------------|--------------|--------|
| 1  | user-1  | 299.98       | 1234567890000| PROCESSED |
| 2  | user-2  | 349.99       | 1234567891000| PROCESSED |

---

## 🎓 最佳实践

1. **幂等性写入**: 数据库写入操作应支持幂等性，避免重复数据
2. **错误重试**: 配置合理的重试机制处理临时故障
3. **监控告警**: 设置关键指标的监控告警 (延迟、错误率等)
4. **数据备份**: 定期备份历史数据表
5. **性能测试**: 在生产环境使用前进行充分的性能测试

---

## 📞 技术支持

如有问题，请查看:

- Flink 官方文档：https://flink.apache.org/docs/
- 项目 README.md
- 联系开发团队
