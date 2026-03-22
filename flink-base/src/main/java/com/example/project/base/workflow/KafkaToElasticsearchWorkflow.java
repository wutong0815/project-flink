package com.example.project.base.workflow;

import com.example.project.base.model.Order;
import com.example.project.core.es.EsSinkFunction;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.HashMap;
import java.util.Map;

/**
 * 完整数据流水线工作流：Kafka → Flink → Elasticsearch
 * 
 * 数据流程:
 * 1. 从 Kafka 读取原始订单数据（orders 主题）
 * 2. 实时处理和转换（过滤、聚合、格式化）
 * 3. 写入 Elasticsearch 进行存储和查询（orders-index 索引）
 * 
 * 适用场景:
 * - 实时订单监控和分析
 * - 电商数据大屏展示
 * - 用户行为追踪
 * 
 * 部署架构:
 * ┌──────────┐    ┌─────────┐    ┌──────────────┐
 * │  Kafka   │ -> │  Flink  │ -> │ ElasticSearch│
 * │ (原始数据)│    │ (处理)  │    │   (存储/查询) │
 * └──────────┘    └─────────┘    └──────────────┘
 */
public class KafkaToElasticsearchWorkflow {
    
    private static final Logger LOG = LoggerFactory.getLogger(KafkaToElasticsearchWorkflow.class);
    private static final ObjectMapper MAPPER = new ObjectMapper();
    
    /**
     * 运行 Kafka 到 ES 的完整工作流
     * 
     * @param kafkaServers Kafka 服务器地址
     * @param kafkaTopic Kafka 输入主题
     * @param esHosts ES 服务器地址
     * @param esIndex ES 索引名称
     */
    public void runWorkflow(
            String kafkaServers,
            String kafkaTopic,
            String esHosts,
            String esIndex) throws Exception {
        
        LOG.info("========================================");
        LOG.info("启动 Kafka → Elasticsearch 工作流");
        LOG.info("========================================");
        LOG.info("Kafka 配置 - Servers: {}, Topic: {}", kafkaServers, kafkaTopic);
        LOG.info("ES 配置 - Hosts: {}, Index: {}", esHosts, esIndex);
        
        // 1. 创建执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(2);
        
        // 2. 创建 Kafka Source
        KafkaSource<String> kafkaSource = KafkaSource.<String>builder()
                .setBootstrapServers(kafkaServers)
                .setTopics(kafkaTopic)
                .setGroupId("flink-kafka-to-es-group")
                .setStartingOffsets(OffsetsInitializer.earliest())
                .setValueOnlyDeserializer(new SimpleStringSchema())
                .build();
        
        // 3. 从 Kafka 读取数据
        DataStream<String> kafkaStream = env.fromSource(
                kafkaSource,
                WatermarkStrategy.<String>forBoundedOutOfOrderness(Duration.ofSeconds(5))
                        .withTimestampAssigner((event, timestamp) -> System.currentTimeMillis()),
                "Kafka Source"
        );
        
        LOG.info("已从 Kafka 读取数据流");
        
        // 4. 数据处理：解析 → 过滤 → 转换
        DataStream<OrderProcessed> processedStream = kafkaStream
                // 解析 JSON
                .map(json -> {
                    JsonNode node = MAPPER.readTree(json);
                    
                    Order order = new Order();
                    order.setId(node.has("orderId") ? node.get("orderId").asText() : "unknown");
                    order.setUserId(node.has("userId") ? node.get("userId").asText() : "unknown");
                    order.setAmount(node.has("amount") ? node.get("amount").asDouble() : 0.0);
                    order.setTimestamp(node.has("timestamp") ? node.get("timestamp").asLong() : System.currentTimeMillis());
                    order.setStatus(node.has("status") ? node.get("status").asText() : "UNKNOWN");
                    
                    return order;
                })
                // 过滤无效订单（金额<=0 或状态为 CANCELLED）
                .filter(order -> order.amount > 0 && !"CANCELLED".equals(order.status))
                // 转换为处理后对象
                .map(order -> new OrderProcessed(
                        order.id,
                        order.userId,
                        order.amount,
                        order.status,
                        order.timestamp,
                        System.currentTimeMillis() // 处理时间
                ));
        
        LOG.info("数据处理完成，准备写入 Elasticsearch");
        
        // 5. 写入 Elasticsearch
        processedStream.addSink(new EsSinkFunction<>(
                esHosts,
                esIndex,
                500,      // 批量大小
                3000L,    // 刷新间隔 (ms)
                order -> {
                    Map<String, Object> doc = new HashMap<>();
                    doc.put("orderId", order.orderId);
                    doc.put("userId", order.userId);
                    doc.put("amount", order.amount);
                    doc.put("status", order.status);
                    doc.put("orderTime", order.orderTime);
                    doc.put("processTime", order.processTime);
                    
                    // 使用 orderId 作为文档 ID（支持幂等写入）
                    return new DocumentWithId(order.orderId, doc);
                }
        ));
        
        // 6. 打印处理结果（用于调试）
        processedStream.print("处理结果");
        
        // 7. 执行作业
        LOG.info("开始执行 Flink 作业...");
        env.execute("Kafka to Elasticsearch Workflow");
    }
    
    /**
     * 简化版：使用默认配置
     */
    public void runWorkflow(String kafkaServers, String kafkaTopic, String esHosts) throws Exception {
        runWorkflow(kafkaServers, kafkaTopic, esHosts, "orders-index");
    }
    
    /**
     * 处理后的订单数据模型
     */
    public static class OrderProcessed {
        public final String orderId;
        public final String userId;
        public final Double amount;
        public final String status;
        public final Long orderTime;
        public final Long processTime;
        
        public OrderProcessed(String orderId, String userId, Double amount, 
                             String status, Long orderTime, Long processTime) {
            this.orderId = orderId;
            this.userId = userId;
            this.amount = amount;
            this.status = status;
            this.orderTime = orderTime;
            this.processTime = processTime;
        }
    }
    
    /**
     * 带 ID 的文档包装类（用于 ES 写入）
     */
    public static class DocumentWithId extends HashMap<String, Object> {
        private final String docId;
        
        public DocumentWithId(String id, Map<String, Object> data) {
            super(data);
            this.docId = id;
            put("id", id);
        }
        
        public String getDocId() {
            return docId;
        }
    }
}
