package com.example.project.core.kafka;

import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Kafka 客户端工具类
 * 
 * 提供 Kafka 连接的基本操作:
 * - 创建 Kafka Source (消费者)
 * - 创建 Kafka Sink (生产者)
 * - 配置管理
 */
public class KafkaClientUtil {
    
    private static final Logger LOG = LoggerFactory.getLogger(KafkaClientUtil.class);
    
    private String bootstrapServers;
    private String groupId;
    private boolean autoOffsetResetEarliest;
    
    /**
     * 构造函数
     * 
     * @param bootstrapServers Kafka 服务器地址，例如：localhost:9092
     * @param groupId 消费者组 ID
     */
    public KafkaClientUtil(String bootstrapServers, String groupId) {
        this(bootstrapServers, groupId, true);
    }
    
    /**
     * 构造函数
     * 
     * @param bootstrapServers Kafka 服务器地址
     * @param groupId 消费者组 ID
     * @param autoOffsetResetEarliest 是否从最早 offset 开始消费
     */
    public KafkaClientUtil(String bootstrapServers, String groupId, boolean autoOffsetResetEarliest) {
        this.bootstrapServers = bootstrapServers;
        this.groupId = groupId;
        this.autoOffsetResetEarliest = autoOffsetResetEarliest;
        LOG.info("Kafka 客户端初始化成功 - Bootstrap Servers: {}, Group ID: {}", 
                bootstrapServers, groupId);
    }
    
    /**
     * 创建 Kafka Source (用于读取数据)
     * 
     * @param topic 主题名称
     * @return KafkaSource 实例
     */
    public KafkaSource<String> createSource(String topic) {
        LOG.info("创建 Kafka Source - Topic: {}", topic);
        
        OffsetsInitializer offsetsInitializer = autoOffsetResetEarliest 
                ? OffsetsInitializer.earliest() 
                : OffsetsInitializer.latest();
        
        return KafkaSource.<String>builder()
                .setBootstrapServers(bootstrapServers)
                .setTopics(topic)
                .setGroupId(groupId)
                .setStartingOffsets(offsetsInitializer)
                .setValueOnlyDeserializer(new SimpleStringSchema())
                .build();
    }
    
    /**
     * 创建 Kafka Source (用于读取多个主题)
     * 
     * @param topics 主题名称数组
     * @return KafkaSource 实例
     */
    public KafkaSource<String> createSource(String... topics) {
        LOG.info("创建 Kafka Source - Topics: {}", String.join(", ", topics));
        
        OffsetsInitializer offsetsInitializer = autoOffsetResetEarliest 
                ? OffsetsInitializer.earliest() 
                : OffsetsInitializer.latest();
        
        return KafkaSource.<String>builder()
                .setBootstrapServers(bootstrapServers)
                .setTopics(topics)
                .setGroupId(groupId)
                .setStartingOffsets(offsetsInitializer)
                .setValueOnlyDeserializer(new SimpleStringSchema())
                .build();
    }
    
    /**
     * 创建 Kafka Sink (用于写入数据)
     * 
     * @param topic 主题名称
     * @return KafkaSink 实例
     */
    public KafkaSink<String> createSink(String topic) {
        LOG.info("创建 Kafka Sink - Topic: {}", topic);
        
        return KafkaSink.<String>builder()
                .setBootstrapServers(bootstrapServers)
                .setRecordSerializer(KafkaRecordSerializationSchema.builder()
                        .setTopic(topic)
                        .setValueSerializationSchema(new SimpleStringSchema())
                        .build())
                .build();
    }
    
    /**
     * 创建 Kafka Sink (自定义序列化)
     * 
     * @param topic 主题名称
     * @param serializationSchema 自定义序列化器
     * @return KafkaSink 实例
     */
    public <T> KafkaSink<T> createSink(String topic, org.apache.flink.api.common.serialization.SerializationSchema<T> serializationSchema) {
        LOG.info("创建 Kafka Sink (自定义序列化) - Topic: {}", topic);
        
        return KafkaSink.<T>builder()
                .setBootstrapServers(bootstrapServers)
                .setRecordSerializer(KafkaRecordSerializationSchema.builder()
                        .setTopic(topic)
                        .setValueSerializationSchema(serializationSchema)
                        .build())
                .build();
    }
    
    /**
     * 获取 Bootstrap Servers 配置
     * 
     * @return Kafka 服务器地址
     */
    public String getBootstrapServers() {
        return bootstrapServers;
    }
    
    /**
     * 获取 Group ID 配置
     * 
     * @return 消费者组 ID
     */
    public String getGroupId() {
        return groupId;
    }
    
    /**
     * 检查配置是否有效
     * 
     * @return true 如果配置有效
     */
    public boolean isValidConfig() {
        if (bootstrapServers == null || bootstrapServers.trim().isEmpty()) {
            LOG.error("Bootstrap Servers 配置无效");
            return false;
        }
        if (groupId == null || groupId.trim().isEmpty()) {
            LOG.error("Group ID 配置无效");
            return false;
        }
        return true;
    }
}
