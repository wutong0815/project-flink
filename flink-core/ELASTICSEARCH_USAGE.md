# Elasticsearch 使用指南

本文档介绍如何在 flink-core 模块中使用 Elasticsearch 客户端。

## 功能概述

`EsClientUtil` 提供了以下 Elasticsearch 操作功能：

### Index 管理
- ✅ 创建索引（支持自定义配置）
- ✅ 删除索引
- ✅ 检查索引是否存在
- ✅ 列出所有索引

### Document 操作
- ✅ 添加单个文档（支持自动生成 ID 或指定 ID）
- ✅ 获取文档
- ✅ 更新文档
- ✅ 删除文档
- ✅ 批量添加文档

### 搜索功能
- ✅ 匹配所有查询
- ✅ 全文本搜索（多字段匹配）

## 快速开始

### 1. 添加依赖

在 `flink-core/pom.xml` 中已包含 Elasticsearch 依赖：

```xml
<dependency>
    <groupId>co.elastic.clients</groupId>
    <artifactId>elasticsearch-java</artifactId>
</dependency>
```

### 2. 基本使用示例

#### 无认证连接

```java
// 初始化客户端
EsClientUtil esClient = new EsClientUtil("http://localhost:9200");

// 验证连接
if (esClient.isValidConfig()) {
    // 创建索引
    esClient.createIndex("my-index");
    
    // 添加文档
    Map<String, Object> doc = new HashMap<>();
    doc.put("title", "Elasticsearch 入门");
    doc.put("author", "张三");
    doc.put("price", 59.99);
    
    String docId = esClient.addDocument("my-index", "doc-001", doc);
    
    // 获取文档
    Map<String, Object> retrievedDoc = esClient.getDocument("my-index", "doc-001");
    
    // 搜索文档
    List<Map<String, Object>> results = esClient.searchAll("my-index");
    
    // 关闭客户端
    esClient.closeClient();
}
```

#### 带认证连接

```java
// 使用用户名和密码连接
EsClientUtil esClient = new EsClientUtil(
    "http://localhost:9200", 
    "elastic", 
    "your_password"
);

// 或者连接多个节点
EsClientUtil esClientMulti = new EsClientUtil(
    "http://node1:9200,http://node2:9200,http://node3:9200",
    "elastic",
    "your_password"
);
```

### 3. 运行示例程序

项目提供了一个完整的示例程序，展示所有功能：

```bash
cd flink-core

# 运行 Elasticsearch 完整示例（需要 Elasticsearch 服务运行）
mvn spring-boot:run -Dspring-boot.run.arguments="es-example"

# 或者只初始化客户端（不执行操作）
mvn spring-boot:run -Dspring-boot.run.arguments="es http://localhost:9200"

# 使用认证连接
mvn spring-boot:run -Dspring-boot.run.arguments="es http://localhost:9200 elastic your_password"
```

## API 参考

### 构造函数

```java
// 无认证
public EsClientUtil(String hosts)

// 带认证
public EsClientUtil(String hosts, String username, String password)
```

**参数说明：**
- `hosts`: Elasticsearch 服务器地址，支持多个节点（逗号分隔），例如：
  - `"http://localhost:9200"`
  - `"http://node1:9200,http://node2:9200"`

### Index 管理方法

#### createIndex

```java
// 创建默认配置的索引
void createIndex(String indexName)

// 创建自定义配置的索引
void createIndex(String indexName, Map<String, Object> settings)
```

**示例：**
```java
// 默认配置（1 个主分片，1 个副本）
esClient.createIndex("my-index");

// 自定义配置
Map<String, Object> settings = new HashMap<>();
settings.put("number_of_shards", 3);
settings.put("number_of_replicas", 1);
esClient.createIndex("my-index", settings);
```

#### deleteIndex

```java
void deleteIndex(String indexName)
```

#### indexExists

```java
boolean indexExists(String indexName)
```

#### listIndices

```java
List<String> listIndices()
```

### Document 操作方法

#### addDocument

```java
// 自动生成 ID
String addDocument(String indexName, Object document)

// 指定 ID
String addDocument(String indexName, String id, Object document)
```

**示例：**
```java
// 自动生成 ID
Map<String, Object> doc = new HashMap<>();
doc.put("name", "测试文档");
String id1 = esClient.addDocument("my-index", doc);

// 指定 ID
String id2 = esClient.addDocument("my-index", "custom-id-001", doc);
```

#### getDocument

```java
Map<String, Object> getDocument(String indexName, String id)
```

#### updateDocument

```java
void updateDocument(String indexName, String id, Map<String, Object> updates)
```

**示例：**
```java
Map<String, Object> updates = new HashMap<>();
updates.put("price", 79.99);
updates.put("stock", 100);
esClient.updateDocument("my-index", "doc-001", updates);
```

#### deleteDocument

```java
void deleteDocument(String indexName, String id)
```

#### bulkAddDocuments

```java
void bulkAddDocuments(String indexName, List<Map<String, Object>> documents)
```

**示例：**
```java
List<Map<String, Object>> docs = new ArrayList<>();

Map<String, Object> doc1 = new HashMap<>();
doc1.put("title", "文档 1");
docs.add(doc1);

Map<String, Object> doc2 = new HashMap<>();
doc2.put("title", "文档 2");
docs.add(doc2);

esClient.bulkAddDocuments("my-index", docs);
```

### 搜索方法

#### searchAll

```java
List<Map<String, Object>> searchAll(String indexName)
```

#### searchByQuery

```java
List<Map<String, Object>> searchByQuery(String indexName, String query)
```

**示例：**
```java
// 搜索所有文档
List<Map<String, Object>> allDocs = esClient.searchAll("my-index");

// 全文本搜索
List<Map<String, Object>> results = esClient.searchByQuery("my-index", "数据库");
```

### 其他方法

#### isValidConfig

```java
boolean isValidConfig()
```

验证配置并测试连接。

#### getClient

```java
ElasticsearchClient getClient()
```

获取底层的 ElasticsearchClient 实例，用于执行高级操作。

#### closeClient

```java
void closeClient()
```

关闭客户端，释放资源。

## 最佳实践

### 1. 资源管理

始终在使用完毕后关闭客户端：

```java
EsClientUtil esClient = null;
try {
    esClient = new EsClientUtil("http://localhost:9200");
    // 执行操作
} finally {
    if (esClient != null) {
        esClient.closeClient();
    }
}
```

### 2. 批量操作

对于大量数据，使用批量操作提高性能：

```java
List<Map<String, Object>> documents = new ArrayList<>();
for (int i = 0; i < 1000; i++) {
    Map<String, Object> doc = new HashMap<>();
    doc.put("field", "value-" + i);
    documents.add(doc);
}
esClient.bulkAddDocuments("my-index", documents);
```

### 3. 错误处理

所有方法都会抛出 RuntimeException，建议在应用层捕获：

```java
try {
    esClient.addDocument("my-index", "doc-001", doc);
} catch (RuntimeException e) {
    LOG.error("添加文档失败", e);
    // 处理错误
}
```

### 4. 索引配置

生产环境建议显式设置分片和副本：

```java
Map<String, Object> settings = new HashMap<>();
settings.put("number_of_shards", 3);      // 根据数据量设置
settings.put("number_of_replicas", 1);    // 高可用
settings.put("refresh_interval", "5s");   // 刷新间隔
esClient.createIndex("production-index", settings);
```

## 与 Flink 集成

可以将 EsClientUtil 用于 Flink 作业的 Sink 或 Source：

```java
// 在 Flink 作业中使用
DataStream<String> stream = ...;

stream.addSink(new RichSinkFunction<String>() {
    private transient EsClientUtil esClient;
    
    @Override
    public void open(Configuration parameters) {
        esClient = new EsClientUtil("http://localhost:9200");
    }
    
    @Override
    public void invoke(String value, Context context) {
        Map<String, Object> doc = parse(value);
        esClient.addDocument("flink-output", doc);
    }
    
    @Override
    public void close() {
        if (esClient != null) {
            esClient.closeClient();
        }
    }
});
```

## 注意事项

1. **版本兼容性**: 使用 Elasticsearch 8.x 版本
2. **连接池**: 客户端内部已实现连接池，无需额外配置
3. **超时设置**: 默认超时为 30 秒，可通过系统属性调整
4. **SSL/TLS**: 生产环境建议使用 HTTPS 连接

## 故障排查

### 连接失败

检查 Elasticsearch 服务是否运行：
```bash
curl http://localhost:9200
```

查看错误日志获取详细信息。

### 认证失败

确保用户名和密码正确，并且用户有足够权限访问索引。

### 索引不存在

在执行文档操作前，确保索引已创建：
```java
if (!esClient.indexExists("my-index")) {
    esClient.createIndex("my-index");
}
```

## 更多资源

- [Elasticsearch 官方文档](https://www.elastic.co/guide/en/elasticsearch/reference/current/index.html)
- [Elasticsearch Java Client 文档](https://www.elastic.co/guide/en/elasticsearch/client/java-api-client/current/index.html)
