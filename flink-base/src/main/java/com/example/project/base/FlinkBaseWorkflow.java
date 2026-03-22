package com.example.project.base;

import com.example.project.base.workflow.BasicWorkflow;
import com.example.project.base.workflow.KafkaStreamWorkflow;
import com.example.project.base.workflow.EsStreamWorkflow;
import com.example.project.base.workflow.KafkaToElasticsearchWorkflow;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Flink 基础工作流应用入口
 * 
 * 统一的 Flink 作业启动类，支持多种工作流模式:
 * - basic: 基础数据处理工作流（DataGenerator 数据源）
 * - kafka: Kafka 流处理工作流（Kafka Source → 处理 → Kafka Sink）
 * - es: Elasticsearch 写入工作流（数据源 → ES Sink）
 * - pipeline: 完整数据流水线（Kafka → Flink → ES）
 * 
 * 使用方式:
 * <pre>{@code
 * # 本地运行
 * java -jar flink-base.jar                    # 默认 basic 模式
 * java -jar flink-base.jar basic              # 基础工作流
 * java -jar flink-base.jar kafka              # Kafka 工作流
 * java -jar flink-base.jar es                 # ES 工作流
 * java -jar flink-base.jar pipeline           # 完整流水线
 * 
 * # 带参数运行
 * java -jar flink-base.jar kafka localhost:9092 orders output
 * java -jar flink-base.jar es http://es:9200
 * java -jar flink-base.jar pipeline localhost:9092 orders http://es:9200
 * 
 * # 提交到 Flink 集群
 * flink run flink-base.jar [arguments...]
 * }</pre>
 */
public class FlinkBaseWorkflow {
    
    private static final Logger LOG = LoggerFactory.getLogger(FlinkBaseWorkflow.class);
    
    /**
     * 主方法
     * 
     * @param args 命令行参数
     *             args[0]: 工作流类型 (basic|kafka|es)，默认 basic
     *             args[1..n]: 各工作流特定参数
     */
    public static void main(String[] args) throws Exception {
        LOG.info("========================================");
        LOG.info("Flink Base Workflow 启动");
        LOG.info("========================================");
        
        // 解析工作流类型
        String workflowType = args.length > 0 ? args[0] : "basic";
        
        // 创建执行环境
        StreamExecutionEnvironment env = createExecutionEnvironment();
        
        // 根据类型运行不同的工作流
        switch (workflowType.toLowerCase()) {
            case "basic":
                runBasicWorkflow(env, args);
                break;
                
            case "kafka":
                runKafkaWorkflow(env, args);
                break;
                
            case "es":
                runEsWorkflow(env, args);
                break;
                
            case "pipeline":
                runPipelineWorkflow(env, args);
                break;
                
            default:
                printUsage();
                throw new IllegalArgumentException("未知的工作流类型：" + workflowType);
        }
        
        // 执行作业
        LOG.info("开始执行 Flink 作业...");
        env.execute("Flink Base Workflow - " + workflowType.toUpperCase());
    }
    
    /**
     * 创建执行环境
     */
    private static StreamExecutionEnvironment createExecutionEnvironment() {
        // 创建基础环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        
        // 设置并行度（生产环境建议从配置文件读取）
        env.setParallelism(2);
        
        // 启用 Checkpoint（生产环境必须）
        // env.enableCheckpointing(60000);
        
        LOG.info("执行环境已创建 - 并行度：{}", env.getParallelism());
        
        return env;
    }
    
    /**
     * 运行基础工作流
     */
    private static void runBasicWorkflow(StreamExecutionEnvironment env, String[] args) {
        LOG.info("运行基础工作流...");
        BasicWorkflow workflow = new BasicWorkflow();
        workflow.runWorkflow(env);
    }
    
    /**
     * 运行 Kafka 工作流
     */
    private static void runKafkaWorkflow(StreamExecutionEnvironment env, String[] args) {
        LOG.info("运行 Kafka 工作流...");
        
        // 解析参数
        String kafkaServers = args.length > 1 ? args[1] : "localhost:9092";
        String inputTopic = args.length > 2 ? args[2] : "orders";
        String outputTopic = args.length > 3 ? args[3] : "order-aggregates";
        
        LOG.info("Kafka 配置 - Servers: {}, Input: {}, Output: {}", 
                kafkaServers, inputTopic, outputTopic);
        
        KafkaStreamWorkflow workflow = new KafkaStreamWorkflow();
        workflow.runKafkaWorkflow(env, kafkaServers, inputTopic, outputTopic);
    }
    
    /**
     * 运行 Elasticsearch 工作流
     */
    private static void runEsWorkflow(StreamExecutionEnvironment env, String[] args) {
        LOG.info("运行 Elasticsearch 工作流...");
        
        // 解析参数
        String esHosts = System.getenv("ES_HOSTS");
       if (esHosts == null || esHosts.isEmpty()) {
           esHosts = args.length > 1 ? args[1] : "http://localhost:9200";
        }
        
        LOG.info("Elasticsearch 配置 - Hosts: {}", esHosts);
        
        EsStreamWorkflow workflow = new EsStreamWorkflow();
        workflow.runEsWorkflow(env, esHosts);
    }
    
    /**
     * 运行完整数据流水线工作流（Kafka → Flink → ES）
     */
    private static void runPipelineWorkflow(StreamExecutionEnvironment env, String[] args) {
        LOG.info("运行完整数据流水线工作流...");
        
        // 解析参数
        String kafkaServers = args.length > 1 ? args[1] : "localhost:9092";
        String kafkaTopic = args.length > 2 ? args[2] : "orders";
        String esHosts = System.getenv("ES_HOSTS");
       if (esHosts == null || esHosts.isEmpty()) {
           esHosts = args.length > 3 ? args[3] : "http://localhost:9200";
        }
        String esIndex = args.length > 4 ? args[4] : "orders-index";
        
        LOG.info("流水线配置 - Kafka: {}/{}, ES: {}/{}", 
                kafkaServers, kafkaTopic, esHosts, esIndex);
        
        try {
            KafkaToElasticsearchWorkflow workflow = new KafkaToElasticsearchWorkflow();
            workflow.runWorkflow(kafkaServers, kafkaTopic, esHosts, esIndex);
        } catch (Exception e) {
            LOG.error("流水线工作流执行失败", e);
            throw new RuntimeException("流水线工作流执行失败", e);
        }
    }
    
    /**
     * 打印使用说明
     */
    private static void printUsage() {
        LOG.info("");
        LOG.info("使用方法:");
        LOG.info("  java -jar flink-base.jar [workflow] [args...]");
        LOG.info("");
        LOG.info("工作流类型:");
        LOG.info("  basic                              - 基础数据处理工作流");
        LOG.info("  kafka [servers] [input] [output]   - Kafka 流处理工作流");
        LOG.info("  es [hosts]                         - Elasticsearch 写入工作流");
        LOG.info("  pipeline [kafka] [topic] [es] [index] - 完整流水线 (Kafka→Flink→ES)");
        LOG.info("");
        LOG.info("示例:");
        LOG.info("  java -jar flink-base.jar basic");
        LOG.info("  java -jar flink-base.jar kafka localhost:9092 orders aggregates");
        LOG.info("  java -jar flink-base.jar es http://elasticsearch:9200");
        LOG.info("  java -jar flink-base.jar pipeline localhost:9092 orders http://es:9200 orders-index");
        LOG.info("");
        LOG.info("环境变量:");
        LOG.info("  ES_HOSTS - Elasticsearch 服务地址（优先于命令行参数）");
        LOG.info("");
    }
}
