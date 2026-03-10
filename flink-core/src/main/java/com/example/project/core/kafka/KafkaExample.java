package com.example.project.core.kafka;

import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Kafka 客户端使用示例
 * 
 * 演示如何使用 FlinkCore 提供的 Kafka 客户端:
 * 1. 创建 Kafka 客户端
 * 2. 创建 Source (消费者)
 * 3. 创建 Sink (生产者)
 */
public class KafkaExample {
    
    private static final Logger LOG = LoggerFactory.getLogger(KafkaExample.class);
    
    /**
     * 运行 Kafka 客户端示例
     */
    public void runExample() {
        try {
            LOG.info("========================================");
            LOG.info("Kafka 客户端使用示例");
            LOG.info("========================================");
            
            // 1. 创建 Kafka 客户端
            String kafkaServers = "localhost:9092";
            String groupId = "flink-example-group";
            
            LOG.info("步骤 1: 创建 Kafka 客户端");
            LOG.info("Bootstrap Servers: {}", kafkaServers);
            LOG.info("Group ID: {}", groupId);
            
            KafkaClientUtil kafkaClient = new KafkaClientUtil(kafkaServers, groupId);
            LOG.info("Kafka 客户端创建成功");
            
            // 2. 验证配置
            LOG.info("\n步骤 2: 验证配置");
            if (kafkaClient.isValidConfig()) {
                LOG.info("配置验证通过 ✓");
            } else {
                LOG.error("配置验证失败 ✗");
                return;
            }
            
            // 3. 创建 Kafka Source (消费者)
            LOG.info("\n步骤 3: 创建 Kafka Source (消费者)");
            String inputTopic = "example-input-topic";
            LOG.info("输入主题：{}", inputTopic);
            
            KafkaSource<String> source = kafkaClient.createSource(inputTopic);
            LOG.info("Kafka Source 创建成功 ✓");
            LOG.info("Source 配置:");
            LOG.info("  - Bootstrap Servers: {}", kafkaClient.getBootstrapServers());
            LOG.info("  - Group ID: {}", kafkaClient.getGroupId());
            
            // 4. 创建多个主题的 Source
            LOG.info("\n步骤 4: 创建多主题 Kafka Source");
            String topic1 = "topic-1";
            String topic2 = "topic-2";
            LOG.info("主题列表：{}, {}", topic1, topic2);
            
            KafkaSource<String> multiTopicSource = kafkaClient.createSource(topic1, topic2);
            LOG.info("多主题 Source 创建成功 ✓");
            
            // 5. 创建 Kafka Sink (生产者)
            LOG.info("\n步骤 5: 创建 Kafka Sink (生产者)");
            String outputTopic = "example-output-topic";
            LOG.info("输出主题：{}", outputTopic);
            
            KafkaSink<String> sink = kafkaClient.createSink(outputTopic);
            LOG.info("Kafka Sink 创建成功 ✓");
            
            // 6. 使用指南
            LOG.info("\n========================================");
            LOG.info("使用指南:");
            LOG.info("========================================");
            LOG.info("1. 从 Source 读取数据:");
            LOG.info("   DataStream<String> stream = env.fromSource(source, ...)");
            LOG.info("");
            LOG.info("2. 将数据写入 Sink:");
            LOG.info("   dataStream.sinkTo(sink);");
            LOG.info("");
            LOG.info("3. 自定义序列化:");
            LOG.info("   KafkaSink<MyType> sink = kafkaClient.createSink(topic, serializationSchema);");
            LOG.info("");
            LOG.info("========================================");
            LOG.info("示例运行完成！");
            LOG.info("========================================");
            
        } catch (Exception e) {
            LOG.error("运行 Kafka 示例时发生错误", e);
            throw new RuntimeException("Kafka 示例运行失败", e);
        }
    }
    
    /**
     * 主方法 - 用于独立运行
     */
    public static void main(String[] args) {
        KafkaExample example = new KafkaExample();
        example.runExample();
    }
}
