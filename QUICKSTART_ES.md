# Elasticsearch 集成 - 快速开始指南

## 5 分钟快速上手

### 前提条件

1. 已安装 Docker 和 Docker Compose
2. 已安装 Maven 3.6+
3. Java 11+

### 步骤 1: 启动 Elasticsearch（本地测试）

```bash
# 使用 Docker 快速启动 ES
docker run -d \
  --name elasticsearch \
  -p 9200:9200 \
  -e "discovery.type=single-node" \
  -e "xpack.security.enabled=false" \
  docker.elastic.co/elasticsearch/elasticsearch:8.11.0

# 等待 ES 启动（约 30 秒）
curl http://localhost:9200
```

### 步骤 2: 编译项目

```bash
cd /Users/wutong/Public/ideaproject/IndependentProject/project-flink

# 编译整个项目
mvn clean install -DskipTests

# 或只编译 flink-base 模块
mvn clean compile -pl flink-base -am
```

### 步骤 3: 运行 ES 工作流

```bash
# 方式 1: 使用环境变量
export ES_HOSTS="http://localhost:9200"
cd flink-base
mvn spring-boot:run -Dspring-boot.run.arguments="es"

# 方式 2: 直接传递参数
mvn spring-boot:run -Dspring-boot.run.arguments="es http://localhost:9200"
```

### 步骤 4: 验证数据

```bash
# 查看 ES 索引
curl http://localhost:9200/orders/_search?pretty

# 应该能看到写入的订单数据
{
  "hits": {
    "hits": [
      {
        "_source": {
          "orderId": "order-1",
          "userId": "user-1",
          "amount": 567.89,
          "status": "COMPLETED"
        }
      }
    ]
  }
}
```

## 使用 Docker Compose 完整部署

创建 `docker-compose.yml`:

```yaml
version: '3.8'
services:
  elasticsearch:
    image: docker.elastic.co/elasticsearch/elasticsearch:8.11.0
   container_name: elasticsearch
   environment:
      - discovery.type=single-node
      - xpack.security.enabled=false
   ports:
      - "9200:9200"
    healthcheck:
      test: ["CMD-SHELL", "curl -f http://localhost:9200 || exit 1"]
      interval: 10s
      timeout: 10s
      retries: 5
  
  flink-jobmanager:
    build:
     context: .
      dockerfile: flink-base/Dockerfile
   container_name: flink-jobmanager
   environment:
      - ES_HOSTS=http://elasticsearch:9200
   ports:
      - "8081:8081"
    depends_on:
      elasticsearch:
       condition: service_healthy
   command: jobmanager
  
  flink-taskmanager:
    build:
     context: .
      dockerfile: flink-base/Dockerfile
   container_name: flink-taskmanager
   environment:
      - ES_HOSTS=http://elasticsearch:9200
    depends_on:
      - flink-jobmanager
      - elasticsearch
   command: taskmanager
```

启动服务：

```bash
docker-compose up -d

# 查看日志
docker-compose logs -f flink-taskmanager

# 访问 Flink UI
# http://localhost:8081
```

## 代码示例：自定义 Workflow

### 1. 创建自己的 Workflow 类

```java
package com.example.project.base.workflow;

import com.example.project.core.es.EsSinkFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.HashMap;
import java.util.Map;

public class MyEsWorkflow {
    
  public void runWorkflow(String esHosts) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
      env.setParallelism(2);
        
        // 创建测试数据流
        DataStream<Map<String, Object>> stream = env.fromSequence(1, 100)
            .map(seq -> {
                Map<String, Object> doc = new HashMap<>();
                doc.put("id", "doc-" + seq);
                doc.put("value", "test-" + seq);
                doc.put("timestamp", System.currentTimeMillis());
                return doc;
            });
        
        // 写入 ES
        stream.addSink(new EsSinkFunction<>(
            esHosts,
            "my-index",
           100,     // bulk size
           2000L,  // flush interval
            doc -> doc  // 直接返回 Map
        ));
        
      env.execute("My ES Workflow");
    }
}
```

### 2. 在 FlinkBase 中注册

编辑 `FlinkBase.java`:

```java
else if ("my-es".equals(args[0])) {
    LOG.info("启动自定义 ES 工作流...");
    MyEsWorkflow workflow = new MyEsWorkflow();
    String esHosts = System.getenv("ES_HOSTS");
  if (esHosts == null) {
        esHosts = args.length > 1 ? args[1] : "http://localhost:9200";
    }
    workflow.runWorkflow(esHosts);
}
```

### 3. 运行

```bash
mvn spring-boot:run -Dspring-boot.run.arguments="my-es"
```

## 常用命令

### 查看 ES 索引

```bash
# 列出所有索引
curl http://localhost:9200/_cat/indices?v

# 查看索引文档数量
curl http://localhost:9200/orders/_count

# 搜索数据
curl -X GET "http://localhost:9200/orders/_search" -H 'Content-Type: application/json' -d'
{
  "query": { "match_all": {} }
}'
```

### 清理数据

```bash
# 删除索引
curl -X DELETE "http://localhost:9200/orders"

# 停止 Docker 容器
docker stop elasticsearch
docker rm elasticsearch
```

## 故障排查

### 问题 1: 连接 ES 失败

**症状**: `Elasticsearch 客户端初始化失败`

**解决**:
```bash
# 检查 ES 是否运行
curl http://localhost:9200

# 检查网络连通性
ping elasticsearch

# 查看 ES 日志
docker logs elasticsearch
```

### 问题 2: 编译错误

**症状**: `找不到包 co.elastic.clients`

**解决**:
```bash
# 重新编译父项目
cd project-flink
mvn clean install -DskipTests

# 检查依赖
cd flink-base
mvn dependency:tree | grep elasticsearch
```

### 问题 3: 数据未写入

**症状**: ES 中查不到数据

**解决**:
```bash
# 检查 Flink 作业状态
curl http://localhost:8081/jobs

# 查看 TaskManager 日志
docker logs flink-taskmanager

# 手动触发刷新（如果设置了定时刷新）
# 等待几秒让缓冲区刷新
```

## 性能测试

### 压测脚本

```bash
#!/bin/bash
# benchmark.sh

echo "开始压测..."
START=$(date +%s)

# 提交 10000 条数据
for i in {1..10000}; do
    curl -X POST "http://localhost:9200/orders/_doc" \
      -H 'Content-Type: application/json' \
      -d "{\"orderId\":\"order-$i\",\"amount\":$RANDOM}" > /dev/null
done

END=$(date +%s)
DURATION=$((END - START))

echo "完成！耗时：${DURATION}秒"
echo "吞吐量：$((10000 / DURATION)) records/秒"
```

### Flink 压测配置

```java
// 增加并行度
env.setParallelism(4);

// 增大批量
new EsSinkFunction<>(..., 5000, 2000L, ...);

// 调整资源
// TaskManager: 4 CPU, 4GB Memory
```

## 下一步学习

1. 阅读 [`ELASTICSEARCH_INTEGRATION.md`](./flink-base/ELASTICSEARCH_INTEGRATION.md) 了解详细集成方案
2. 阅读 [`kubernetes-es-deployment.yaml`](./flink-base/kubernetes-es-deployment.yaml) 学习 K8s 部署
3. 查看 [`FLINK_ES_INTEGRATION_SUMMARY.md`](./FLINK_ES_INTEGRATION_SUMMARY.md) 了解架构设计

## 参考资源

- [Elasticsearch 官方文档](https://www.elastic.co/guide/)
- [Flink Connector 文档](https://nightlies.apache.org/flink/flink-docs-stable/docs/connectors/datastream/elasticsearch/)
- [项目 README](./README.md)

---

**祝你使用愉快！** 🎉
