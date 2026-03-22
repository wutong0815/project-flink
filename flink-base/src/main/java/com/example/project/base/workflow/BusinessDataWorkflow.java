package com.example.project.base.workflow;

import com.example.project.base.model.Order;
import com.example.project.core.es.EsSinkFunction;
import com.example.project.core.jdbc.JdbcClientUtil;
import com.example.project.core.kafka.KafkaClientUtil;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * 完整业务工作流：数据库 → Kafka → Flink(ES 增强) → 数据库
 * 
 * 业务流程:
 * 1. 从数据库读取原始订单数据
 * 2. 将数据发送到 Kafka 进行消息队列管理
 * 3. Flink 从 Kafka 消费数据进行实时处理
 * 4. 处理过程中查询 ES 获取用户画像等增强信息
 * 5. 将处理后的结果写入数据库作为历史数据
 * 
 * 适用场景:
 * - 电商订单实时分析
 * - 用户行为追踪与历史归档
 * - 实时风控与数据存储
 */
public class BusinessDataWorkflow {
    
    private static final Logger LOG = LoggerFactory.getLogger(BusinessDataWorkflow.class);
    private static final ObjectMapper MAPPER = new ObjectMapper();
    
    /**
     * 运行完整业务工作流
     * 
     * @param dbUrl 数据库 JDBC URL
     * @param dbUser 数据库用户名
     * @param dbPassword 数据库密码
     * @param kafkaServers Kafka 服务器地址
     * @param esHosts Elasticsearch 服务器地址
     * @param inputTopic 输入 Kafka Topic
     * @param outputTopic 输出 Kafka Topic
     */
    public void runBusinessWorkflow(String dbUrl, String dbUser, String dbPassword,
                                   String kafkaServers, String esHosts,
                                   String inputTopic, String outputTopic) throws Exception {
        
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        
        // 启用检查点以支持 Exactly-Once 语义
        env.enableCheckpointing(5000);
        
        runBusinessWorkflow(env, dbUrl, dbUser, dbPassword, kafkaServers, esHosts, inputTopic, outputTopic);
    }
    
    /**
     * 运行完整业务工作流 (带环境参数)
     */
    public void runBusinessWorkflow(StreamExecutionEnvironment env,
                                   String dbUrl, String dbUser, String dbPassword,
                                   String kafkaServers, String esHosts,
                                   String inputTopic, String outputTopic) throws Exception {
        
        LOG.info("========================================");
        LOG.info("启动完整业务工作流");
        LOG.info("========================================");
        LOG.info("数据库：{}", dbUrl);
        LOG.info("Kafka: {}", kafkaServers);
        LOG.info("Elasticsearch: {}", esHosts);
        LOG.info("输入 Topic: {}, 输出 Topic: {}", inputTopic, outputTopic);
        
        // ========== 阶段 1: 从数据库读取数据并发送到 Kafka ==========
        LOG.info("\n阶段 1: 从数据库读取数据并发送到 Kafka...");
        loadDataFromDatabaseToKafka(env, dbUrl, dbUser, dbPassword, kafkaServers, inputTopic);
        
        // ========== 阶段 2: 从 Kafka 消费数据并进行流处理 ==========
        LOG.info("\n阶段 2: 从 Kafka 消费数据并进行流处理...");
        DataStream<OrderResult> processedStream = processDataFromKafka(env, kafkaServers, inputTopic, esHosts);
        
        // ========== 阶段 3: 将处理结果写入数据库 ==========
        LOG.info("\n阶段 3: 将处理结果写入数据库...");
        writeResultsToDatabase(processedStream, dbUrl, dbUser, dbPassword);
        
        // ========== 执行作业 ==========
        LOG.info("\n========================================");
        LOG.info("开始执行 Flink 作业...");
        LOG.info("========================================");
        env.execute("Business Data Workflow");
    }
    
    /**
     * 阶段 1: 从数据库加载数据到 Kafka
     */
    private void loadDataFromDatabaseToKafka(StreamExecutionEnvironment env,
                                             String dbUrl, String dbUser, String dbPassword,
                                             String kafkaServers, String topic) throws Exception {
        
        LOG.info("从数据库加载数据到 Kafka...");
        
        // 创建数据库连接
        JdbcClientUtil jdbcClient = new JdbcClientUtil(dbUrl, dbUser, dbPassword, "com.mysql.cj.jdbc.Driver");
        
        // 创建 Kafka Sink
        KafkaClientUtil kafkaClient = new KafkaClientUtil(kafkaServers, "business-workflow-producer");
        KafkaSink<String> sink = kafkaClient.createSink(topic);
        
        // 从数据库读取数据
        List<Map<String, Object>> orders = jdbcClient.query(
            "SELECT order_id, user_id, amount, product, status, create_time FROM orders WHERE status = ?", 
            "PENDING"
        );
        
        LOG.info("从数据库读取到 {} 条订单数据", orders.size());
        
        // 转换为数据流并发送到 Kafka
        DataStream<String> orderStream = env.fromCollection(orders)
            .map(order -> {
                JsonNode node = MAPPER.valueToTree(order);
                return MAPPER.writeValueAsString(order);
            });
        
        // 写入 Kafka
        orderStream.sinkTo(sink);
        
        LOG.info("数据已成功发送到 Kafka Topic: {}", topic);
    }
    
    /**
     * 阶段 2: 从 Kafka 消费数据并进行处理 (包含 ES 查询增强)
     */
    private DataStream<OrderResult> processDataFromKafka(StreamExecutionEnvironment env,
                                                         String kafkaServers, String inputTopic,
                                                         String esHosts) throws Exception {
        
        LOG.info("从 Kafka 消费数据并进行处理...");
        
        // 创建 Kafka Source
        KafkaClientUtil kafkaClient = new KafkaClientUtil(kafkaServers, "business-workflow-consumer");
        KafkaSource<String> source = kafkaClient.createSource(inputTopic);
        
        // 从 Kafka 读取数据
        DataStream<String> kafkaStream = env.fromSource(
                source,
                WatermarkStrategy.<String>forBoundedOutOfOrderness(Duration.ofSeconds(5))
                    .withTimestampAssigner((event, timestamp) -> {
                        try {
                            JsonNode node = MAPPER.readTree(event);
                            return node.has("timestamp") ? node.get("timestamp").asLong() : timestamp;
                        } catch (Exception e) {
                            return timestamp;
                        }
                    }),
                "Kafka Source"
        );
        
        // 数据处理流程
        DataStream<OrderResult> resultStream = kafkaStream
            // 1. 解析 JSON 数据
            .map(json -> {
                JsonNode node = MAPPER.readTree(json);
                Order order = new Order(
                    node.has("order_id") ? node.get("order_id").asText() : "",
                    node.has("user_id") ? node.get("user_id").asText() : "",
                    node.has("amount") ? node.get("amount").asDouble() : 0.0,
                    node.has("product") ? node.get("product").asText() : "",
                    node.has("create_time") ? node.get("create_time").asLong() : System.currentTimeMillis()
                );
                LOG.debug("解析订单：{}", order);
                return order;
            })
            
            // 2. 过滤无效订单 (金额 <= 0)
            .filter(order -> order.amount > 0)
            
            // 3. 数据增强：查询 ES 获取用户信息 (示例中简化为模拟数据)
            .map(new MapFunction<Order, Tuple2<Order, UserInfo>>() {
                @Override
                public Tuple2<Order, UserInfo> map(Order order) throws Exception {
                    // TODO: 实际场景中这里会查询 ES
                    // UserInfo userInfo = esClient.getUserInfo(order.userId);
                    
                    // 示例：模拟用户信息
                    UserInfo userInfo = new UserInfo();
                    userInfo.userId = order.userId;
                    userInfo.userLevel = order.amount > 1000 ? "VIP" : "NORMAL";
                    userInfo.riskScore = (int) (Math.random() * 100);
                    
                    return Tuple2.of(order, userInfo);
                }
            }, Types.TUPLE(Types.POJO, Types.POJO))
            
            // 4. 业务规则处理：根据用户等级和风险评分进行过滤
            .filter(tuple -> {
                Order order = tuple.f0;
                UserInfo userInfo = tuple.f1;
                
                // VIP 用户直接通过
                if ("VIP".equals(userInfo.userLevel)) {
                    return true;
                }
                
                // 普通用户：金额 < 500 且风险评分 < 80 才通过
                return order.amount < 500 && userInfo.riskScore < 80;
            })
            
            // 5. 按用户分组并聚合统计
            .map(tuple -> Tuple2.of(tuple.f0.userId, tuple.f0.amount))
            .keyBy(value -> value.f0)
            .sum(1)
            
            // 6. 转换为结果对象
            .map(tuple -> {
                OrderResult result = new OrderResult();
                result.userId = tuple.f0;
                result.totalAmount = tuple.f1;
                result.processTime = System.currentTimeMillis();
                result.status = "PROCESSED";
                return result;
            });
        
        // 打印处理结果
        resultStream.print("处理结果");
        
        return resultStream;
    }
    
    /**
     * 阶段 3: 将处理结果写入数据库
     */
    private void writeResultsToDatabase(DataStream<OrderResult> resultStream,
                                       String dbUrl, String dbUser, String dbPassword) throws Exception {
        
        LOG.info("将处理结果写入数据库...");
        
        resultStream.addSink(new org.apache.flink.streaming.api.functions.sink.RichSinkFunction<OrderResult>() {
            private Connection conn;
            private PreparedStatement pstmt;
            private JdbcClientUtil jdbcClient;
            
            @Override
            public void open(org.apache.flink.configuration.Configuration parameters) throws Exception {
                super.open(parameters);
                jdbcClient = new JdbcClientUtil(dbUrl, dbUser, dbPassword, "com.mysql.cj.jdbc.Driver");
                conn = jdbcClient.getConnection();
                
                // 准备插入语句
                String sql = "INSERT INTO order_history (user_id, total_amount, process_time, status) VALUES (?, ?, ?, ?)";
                pstmt = conn.prepareStatement(sql);
                
                LOG.info("数据库连接已建立，准备写入数据");
            }
            
            @Override
            public void invoke(OrderResult result, Context context) throws Exception {
                pstmt.setString(1, result.userId);
                pstmt.setDouble(2, result.totalAmount);
                pstmt.setLong(3, result.processTime);
                pstmt.setString(4, result.status);
                pstmt.executeUpdate();
                
                LOG.debug("写入历史记录：{}", result);
            }
            
            @Override
            public void close() throws Exception {
                if (pstmt != null) pstmt.close();
                if (conn != null) conn.close();
                if (jdbcClient != null) jdbcClient.close();
                super.close();
            }
        });
        
        LOG.info("结果写入 Sink 已配置完成");
    }
    
    /**
     * 用户信息模型 (用于 ES 查询增强)
     */
    public static class UserInfo {
        public String userId;
        public String userLevel;      // 用户等级：VIP/NORMAL
        public int riskScore;         // 风险评分：0-100
        
        @Override
        public String toString() {
            return "UserInfo{" +
                    "userId='" + userId + '\'' +
                    ", userLevel='" + userLevel + '\'' +
                    ", riskScore=" + riskScore +
                    '}';
        }
    }
    
    /**
     * 订单处理结果模型
     */
    public static class OrderResult {
        public String userId;
        public Double totalAmount;
        public Long processTime;
        public String status;
        
        @Override
        public String toString() {
            return "OrderResult{" +
                    "userId='" + userId + '\'' +
                    ", totalAmount=" + totalAmount +
                    ", processTime=" + processTime +
                    ", status='" + status + '\'' +
                    '}';
        }
    }
}
