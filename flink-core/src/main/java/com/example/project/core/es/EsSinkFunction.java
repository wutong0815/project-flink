package com.example.project.core.es;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Elasticsearch Sink Function（Flink 算子）
 * 
 * 用于 Flink 作业中实时写入数据到 Elasticsearch：
 * - 继承 RichSinkFunction，支持 Flink 生命周期管理
 * - 内置批量缓冲和定时刷新机制
 * - 每个并行子任务独立维护一个 EsWriter 实例
 * 
 * 使用示例:
 * <pre>{@code
 * DataStream<Order> orders = ...;
 * 
 * orders.addSink(new EsSinkFunction<>(
 *     "http://es-pod:9200",  // ES 地址
 *     "orders",              // 索引名称
 *     1000,                  // 批量大小
 *     5000L,                 // 刷新间隔 (ms)
 *     order -> {             // 转换函数
 *         Map<String, Object> doc = new HashMap<>();
 *         doc.put("orderId", order.getId());
 *         doc.put("amount", order.getAmount());
 *         doc.put("timestamp", order.getTimestamp());
 *         return doc;
 *     }
 * ));
 * }</pre>
 * 
 * @param <T> 输入数据类型
 */
public class EsSinkFunction<T> extends RichSinkFunction<T> {
    
    private static final Logger LOG = LoggerFactory.getLogger(EsSinkFunction.class);
    
    // 配置参数
    private final String esHosts;
    private final String indexName;
    private final int bulkSize;
    private final long flushIntervalMs;
    private final DocumentMapper<T> mapper;
    
    // 运行时状态
    private transient EsWriter esWriter;
    private transient List<Map<String, Object>> buffer;
    private transient long lastFlushTime;
    private transient int recordCount;
    
    /**
     * 文档映射接口（将输入数据转换为 ES 文档）
     */
    @FunctionalInterface
  public interface DocumentMapper<T> extends Serializable {
        Map<String, Object> map(T value);
    }
    
    /**
     * 构造函数
     * 
     * @param esHosts ES 服务器地址
     * @param indexName 索引名称
     * @param bulkSize 批量大小
     * @param flushIntervalMs 刷新间隔
     * @param mapper 文档映射函数
     */
  public EsSinkFunction(String esHosts, String indexName, int bulkSize, long flushIntervalMs, DocumentMapper<T> mapper) {
        this.esHosts = esHosts;
        this.indexName = indexName;
        this.bulkSize = bulkSize;
        this.flushIntervalMs = flushIntervalMs;
        this.mapper = mapper;
        LOG.info("EsSinkFunction 初始化 - Index: {}, BulkSize: {}, FlushInterval: {}ms", 
                indexName, bulkSize, flushIntervalMs);
    }
    
    /**
     * 构造函数（默认配置：bulkSize=1000, flushInterval=5000ms）
     * 
     * @param esHosts ES 服务器地址
     * @param indexName 索引名称
     * @param mapper 文档映射函数
     */
  public EsSinkFunction(String esHosts, String indexName, DocumentMapper<T> mapper) {
        this(esHosts, indexName, 1000, 5000L, mapper);
    }
    
    @Override
  public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        LOG.info("打开 EsSinkFunction - Task: {}, Index: {}", 
                getRuntimeContext().getIndexOfThisSubtask(), indexName);
        
        // 初始化 EsWriter
        esWriter = new EsWriter(esHosts, bulkSize, flushIntervalMs);
        
        // 初始化缓冲区
        buffer= new ArrayList<>(bulkSize);
        lastFlushTime = System.currentTimeMillis();
        recordCount = 0;
    }
    
    @Override
  public void invoke(T value, Context context) throws Exception {
        // 转换为 ES 文档
        Map<String, Object> document = mapper.map(value);
      if (document == null || document.isEmpty()) {
            LOG.warn("跳过空文档 - Task: {}", getRuntimeContext().getIndexOfThisSubtask());
            return;
        }
        
        // 添加到缓冲区
        synchronized (buffer) {
          if (context != null && context.timestamp() != null) {
                // 如果有事件时间，添加到文档
                document.put("@timestamp", context.timestamp());
            }
            
            buffer.add(document);
            recordCount++;
            
            // 检查是否需要刷新
          if (shouldFlush()) {
                flushBuffer();
            }
        }
    }
    
    /**
     * 判断是否应该刷新缓冲区
     */
    private boolean shouldFlush() {
       return buffer.size() >= bulkSize || 
               (System.currentTimeMillis() - lastFlushTime >= flushIntervalMs && !buffer.isEmpty());
    }
    
    /**
     * 刷新缓冲区
     */
    private void flushBuffer() {
      if (buffer.isEmpty()) {
            return;
        }
        
        try {
            // 构建批量写入数据
            List<Map<String, Object>> bulkData = new ArrayList<>(buffer.size());
            for (Map<String, Object> doc : buffer) {
                Map<String, Object> entry = new HashMap<>();
              if (doc.containsKey("id")) {
                    entry.put("id", doc.remove("id"));
                } else {
                    entry.put("id", null);
                }
                entry.put("data", doc);
                bulkData.add(entry);
            }
            
            // 批量写入
            esWriter.bulkWrite(indexName, bulkData);
            
            // 清空缓冲区
            buffer.clear();
            lastFlushTime = System.currentTimeMillis();
            
            LOG.debug("缓冲区刷新成功 - Task: {}, Records: {}", 
                    getRuntimeContext().getIndexOfThisSubtask(), recordCount);
            recordCount = 0;
            
        } catch (Exception e) {
            LOG.error("刷新缓冲区失败 - Task: {}", getRuntimeContext().getIndexOfThisSubtask(), e);
            throw new RuntimeException("刷新缓冲区失败", e);
        }
    }
    
    @Override
  public void close() throws Exception {
        // 刷新剩余数据
      if (!buffer.isEmpty()) {
            LOG.info("关闭前刷新剩余数据 - Task: {}, Records: {}", 
                    getRuntimeContext().getIndexOfThisSubtask(), buffer.size());
            flushBuffer();
        }
        
        // 关闭客户端
      if (esWriter != null) {
            esWriter.close();
        }
        
        super.close();
        LOG.info("EsSinkFunction 已关闭 - Task: {}", getRuntimeContext().getIndexOfThisSubtask());
    }
}
