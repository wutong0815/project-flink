package com.example.project.base.workflow;

import com.example.project.base.model.Order;
import com.example.project.base.model.OrderAgg;
import com.example.project.base.util.FileSystemConfigLoader;
import com.example.project.core.kafka.KafkaClientUtil;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Kafka 数据流处理工作流
 * 
 * 演示了:
 * 1. 使用 FlinkCore 提供的 Kafka 客户端创建 Source 和 Sink
 * 2. 从 Kafka 读取数据
 * 3. 实时数据处理和转换
 * 4. 将结果写入 Kafka
 */
public class KafkaStreamWorkflow {
    
    private static final Logger LOG = LoggerFactory.getLogger(KafkaStreamWorkflow.class);
    private static final ObjectMapper MAPPER = new ObjectMapper();
    
    /**
     * 运行 Kafka 流处理工作流
     * 
     * @param kafkaBootstrapServers Kafka 服务器地址
     * @param inputTopic 输入主题
     * @param outputTopic 输出主题
     */
    public void runKafkaWorkflow(String kafkaBootstrapServers, String inputTopic, String outputTopic) throws Exception {
        runKafkaWorkflow(kafkaBootstrapServers, inputTopic, outputTopic, null);
    }
    
    /**
     * 运行 Kafka 流处理工作流（带配置）
     * 
     * @param kafkaBootstrapServers Kafka 服务器地址
     * @param inputTopic 输入主题
     * @param outputTopic 输出主题
     * @param config Flink 配置对象（可选）
     */
    public void runKafkaWorkflow(String kafkaBootstrapServers, String inputTopic, String outputTopic, Configuration config) throws Exception {
        // 1. 创建执行环境
        StreamExecutionEnvironment env;
        if (config != null) {
            env = StreamExecutionEnvironment.getExecutionEnvironment(config);
            LOG.info("使用自定义配置创建执行环境");
        } else {
            // 自动加载文件系统配置
            LOG.info("加载默认文件系统配置...");
            Configuration flinkConfig = FileSystemConfigLoader.createDefaultConfig();
            env = StreamExecutionEnvironment.getExecutionEnvironment(flinkConfig);
        }
        
        env.setParallelism(1);
        
        // 2. 使用 FlinkCore 提供的 Kafka 客户端创建 Source 和 Sink
        LOG.info("初始化 Kafka 客户端...");
        KafkaClientUtil kafkaClient = new KafkaClientUtil(kafkaBootstrapServers, "flink-workflow-group");
        
        // 3. 创建 Kafka Source
        KafkaSource<String> source = kafkaClient.createSource(inputTopic);
        
        // 4. 创建 Kafka Sink
        KafkaSink<String> sink = kafkaClient.createSink(outputTopic);
        
        // 5. 从 Kafka 读取数据
        DataStream<String> kafkaStream = env.fromSource(
                source,
                WatermarkStrategy.noWatermarks(),
                "Kafka Source"
        );
        
        // 6. 数据处理流程：解析 -> 过滤 -> 映射 -> 聚合
        DataStream<OrderAgg> aggregatedStream = kafkaStream
                // 解析 JSON 数据
                .map(json -> {
                    JsonNode node = MAPPER.readTree(json);
                    return new Order(
                            node.get("orderId").asText(),
                            node.get("userId").asText(),
                            node.get("amount").asDouble(),
                            node.get("product").asText(),
                            node.get("timestamp").asLong()
                    );
                })
                // 过滤大额订单 (金额 > 1000)
                .filter(order -> order.amount > 1000)
                // 转换为 (userId, amount) 元组 - 使用匿名类避免泛型类型推断问题
                .map(new MapFunction<Order, Tuple2<String, Double>>() {
                    @Override
                    public Tuple2<String, Double> map(Order order) {
                        return Tuple2.of(order.userId, order.amount);
                    }
                }, Types.TUPLE(Types.STRING, Types.DOUBLE))
                // 按用户分组并计算总金额
                .keyBy(value -> value.f0)
                .sum(1)
                // 转换为聚合对象
                .map(tuple -> new OrderAgg(tuple.f0, tuple.f1));
        
        // 6. 将结果写入 Kafka
        aggregatedStream
                .map(agg -> String.format("{\"userId\":\"%s\",\"totalAmount\":%.2f}", agg.userId, agg.totalAmount))
                .sinkTo(sink);
        
        // 8. 同时打印结果到控制台
        aggregatedStream.print("聚合结果");
        
        // 9. 执行作业
        LOG.info("开始执行 Kafka 流处理工作流...");
        LOG.info("Kafka 服务器：{}", kafkaBootstrapServers);
        LOG.info("输入主题：{}, 输出主题：{}", inputTopic, outputTopic);
        env.execute("Kafka Stream Workflow");
    }
    
    /**
     * 主方法 - 用于独立运行
     */
    public static void main(String[] args) throws Exception {
        // 默认配置
        String kafkaServers = args.length > 0 ? args[0] : "localhost:9092";
        String inputTopic = args.length > 1 ? args[1] : "orders";
        String outputTopic = args.length > 2 ? args[2] : "order-aggregates";
        
        KafkaStreamWorkflow workflow = new KafkaStreamWorkflow();
        workflow.runKafkaWorkflow(kafkaServers, inputTopic, outputTopic);
    }
}
