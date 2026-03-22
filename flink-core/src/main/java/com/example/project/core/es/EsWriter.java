package com.example.project.core.es;

import org.apache.http.HttpHost;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.CredentialsProvider;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.apache.http.impl.nio.client.HttpAsyncClientBuilder;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestClientBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.util.Map;

/**
 * Elasticsearch 写入器（轻量级，支持序列化）
 * 
 * 专为 Flink 作业设计，可在 Sink Function 中使用：
 * - 实现 Serializable，支持在 Flink 算子中序列化传输
 * - 懒加载连接，避免序列化问题
 * - 支持批量写入优化性能
 * 
 * 使用场景：FlinkBase 模块的 Workflow 中调用，实时写入数据到 ES
 */
public class EsWriter implements Serializable {
    
    private static final Logger LOG = LoggerFactory.getLogger(EsWriter.class);
    
    // 配置参数（可序列化）
    private final String hosts;
    private final String username;
    private final String password;
    private final int bulkSize;
    private final long flushIntervalMs;
    
    // 运行时对象（transient，不序列化）
    private transient RestClient restClient;
    private transient co.elastic.clients.elasticsearch.ElasticsearchClient esClient;
    private transient volatile boolean initialized = false;
    
    /**
     * 构造函数（无认证）
     * 
     * @param hosts ES 服务器地址，例如：http://es-pod:9200
     * @param bulkSize 批量写入大小（条数）
     * @param flushIntervalMs 刷新间隔（毫秒）
     */
   public EsWriter(String hosts, int bulkSize, long flushIntervalMs) {
        this(hosts, null, null, bulkSize, flushIntervalMs);
    }
    
    /**
     * 构造函数（带认证）
     * 
     * @param hosts ES 服务器地址
     * @param username 用户名
     * @param password 密码
     * @param bulkSize 批量写入大小
     * @param flushIntervalMs 刷新间隔
     */
   public EsWriter(String hosts, String username, String password, int bulkSize, long flushIntervalMs) {
        this.hosts = hosts;
        this.username = username;
        this.password = password;
        this.bulkSize = bulkSize;
        this.flushIntervalMs = flushIntervalMs;
        LOG.info("EsWriter 初始化配置 - Hosts: {}, BulkSize: {}, FlushInterval: {}ms", 
                hosts, bulkSize, flushIntervalMs);
    }
    
    /**
     * 懒加载初始化客户端（在 Flink 算子中调用）
     */
    private void initClient() {
       if (initialized) {
            return;
        }
        
        synchronized (this) {
           if (initialized) {
                return;
            }
            
            try {
                LOG.info("初始化 Elasticsearch 客户端...");
                
                // 解析 hosts 地址
                String[] hostArray = hosts.split(",");
                HttpHost[] httpHosts = new HttpHost[hostArray.length];
                
                for (int i = 0; i < hostArray.length; i++) {
                    String host = hostArray[i].trim();
                   if (host.startsWith("http://")) {
                        host = host.substring(7);
                    } else if (host.startsWith("https://")) {
                        host = host.substring(8);
                    }
                    
                    String[] parts = host.split(":");
                    String hostname = parts[0];
                    int port = parts.length > 1 ? Integer.parseInt(parts[1]) : 9200;
                    String scheme = hosts.startsWith("https") ? "https" : "http";
                    
                    httpHosts[i] = new HttpHost(hostname, port, scheme);
                }
                
                // 创建 RestClient
                RestClientBuilder restClientBuilder= RestClient.builder(httpHosts);
                
                // 如果提供了用户名和密码，配置认证
               if (username != null && !username.isEmpty() && password != null && !password.isEmpty()) {
                    final CredentialsProvider credentialsProvider= new BasicCredentialsProvider();
                    credentialsProvider.setCredentials(AuthScope.ANY,
                            new UsernamePasswordCredentials(username, password));
                    
                    restClientBuilder.setHttpClientConfigCallback(httpClientBuilder -> {
                        httpClientBuilder.setDefaultCredentialsProvider(credentialsProvider);
                        // 设置超时和连接池
                        httpClientBuilder.setMaxConnTotal(50);
                        httpClientBuilder.setMaxConnPerRoute(20);
                        return httpClientBuilder;
                    });
                }
                
                this.restClient = restClientBuilder.build();
                
                // 创建传输层和客户端
                var transport = new co.elastic.clients.transport.rest_client.RestClientTransport(
                        restClient, 
                        new co.elastic.clients.json.jackson.JacksonJsonpMapper()
                );
                this.esClient = new co.elastic.clients.elasticsearch.ElasticsearchClient(transport);
                
                // 测试连接
                var info = esClient.info();
                LOG.info("Elasticsearch 连接成功 - Cluster: {}, Version: {}", 
                        info.clusterName(), info.version().number());
                
                this.initialized = true;
                
            } catch (Exception e) {
                LOG.error("Elasticsearch 客户端初始化失败", e);
                throw new RuntimeException("Elasticsearch 客户端初始化失败", e);
            }
        }
    }
    
    /**
     * 写入单个文档
     * 
     * @param indexName 索引名称
     * @param id 文档 ID（null 表示自动生成）
     * @param document 文档内容（Map 或自定义对象）
     */
   public void write(String indexName, String id, Object document) {
        ensureInitialized();
        
        try {
            var builder = new co.elastic.clients.elasticsearch.core.IndexRequest.Builder<Object>()
                    .index(indexName)
                    .document(document);
            
           if (id != null && !id.isEmpty()) {
                builder.id(id);
            }
            
            var response = esClient.index(builder.build());
            LOG.debug("文档写入成功 - Index: {}, ID: {}, Result: {}", 
                    indexName, response.id(), response.result());
            
        } catch (Exception e) {
            LOG.error("写入文档失败：{}/{}", indexName, id, e);
            throw new RuntimeException("写入文档失败", e);
        }
    }
    
    /**
     * 批量写入文档
     * 
     * @param indexName 索引名称
     * @param documents 文档列表（每个元素是包含 id 和 document 的 Map）
     *                  格式：[{"id": "doc-1", "data": {...}}, {"id": "doc-2", "data": {...}}]
     */
   public void bulkWrite(String indexName, java.util.List<Map<String, Object>> documents) {
        ensureInitialized();
        
        try {
            var bulkBuilder= new co.elastic.clients.elasticsearch.core.BulkRequest.Builder();
            
            for (Map<String, Object> entry : documents) {
                String id = (String) entry.get("id");
                Object data = entry.get("data");
                
                var opBuilder = new co.elastic.clients.elasticsearch.core.bulk.BulkOperation.Builder()
                        .index(idx -> idx
                                .index(indexName)
                                .document(data));
                
               if (id != null && !id.isEmpty()) {
                    idx.id(id);
                }
                
                bulkBuilder.operations(opBuilder.build());
            }
            
            var response = esClient.bulk(bulkBuilder.build());
            
           if (response.errors()) {
                LOG.error("批量写入失败，部分操作失败");
                for (var item : response.items()) {
                   if (item.error() != null) {
                        LOG.error("操作失败 - ID: {}, Error: {}", item.id(), item.error().reason());
                    }
                }
                throw new RuntimeException("批量写入失败，部分操作失败");
            } else {
                LOG.info("批量写入成功 - Index: {}, 数量：{}", indexName, documents.size());
            }
            
        } catch (Exception e) {
            LOG.error("批量写入失败：{}", indexName, e);
            throw new RuntimeException("批量写入失败", e);
        }
    }
    
    /**
     * 确保客户端已初始化
     */
    private void ensureInitialized() {
       if (!initialized) {
            initClient();
        }
    }
    
    /**
     * 获取 Hosts 配置
     */
   public String getHosts() {
        return hosts;
    }
    
    /**
     * 关闭客户端（在 Flink close() 方法中调用）
     */
   public void close() {
       if (restClient != null) {
            try {
                restClient.close();
                LOG.info("Elasticsearch 客户端已关闭");
            } catch (Exception e) {
                LOG.error("关闭客户端失败", e);
            }
        }
    }
}
