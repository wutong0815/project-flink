package com.example.project.base;

import com.example.project.base.workflow.BasicWorkflow;
import com.example.project.base.workflow.KafkaStreamWorkflow;
import com.example.project.base.workflow.EsStreamWorkflow;
import com.example.project.base.workflow.BusinessDataWorkflow;
import com.example.project.base.util.FileSystemConfigLoader;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

/**
 * Flink 基础应用入口
 * 
 * 支持通过命令行参数选择执行不同的工作流:
 * - 无参数：运行基础工作流 (DataGenerator)
 * - kafka: 运行 Kafka 流处理工作流
 * - basic: 运行基础工作流
 * - es: 运行 Elasticsearch 流处理工作流
 * - business: 运行完整业务工作流 (数据库→Kafka→Flink(ES)→数据库)
 * - jdbc: 运行 JDBC 示例
 */
@SpringBootApplication
public class FlinkBase implements CommandLineRunner {
    
    private static final Logger LOG = LoggerFactory.getLogger(FlinkBase.class);
    
    public static void main(String[] args) {
        SpringApplication.run(FlinkBase.class, args);
    }
    
    @Override
    public void run(String... args) throws Exception {
        // 初始化文件系统配置
        LOG.info("初始化文件系统配置...");
        Configuration flinkConfig = FileSystemConfigLoader.createDefaultConfig();
        
        // 创建流执行环境
        StreamExecutionEnvironment env = createStreamExecutionEnvironment(flinkConfig);
        
        // 根据命令行参数选择执行的工作流
        if (args.length == 0 || "basic".equals(args[0])) {
            runBasicWorkflow(env);
        } else if ("kafka".equals(args[0])) {
            runKafkaWorkflow(env, args);
        } else if ("es".equals(args[0])) {
            runEsWorkflow(env, args);
        } else if ("business".equals(args[0])) {
            runBusinessWorkflow(env, args);
        } else if ("jdbc".equals(args[0])) {
            runJdbcExample(args);
        } else if ("config".equals(args[0])) {
            showConfigInfo(flinkConfig);
        } else {
            LOG.error("未知的工作流类型：{}", args[0]);
            printUsage();
        }
    }
    
    /**
     * 创建流执行环境
     * 
     * @param config Flink 配置对象
     * @return StreamExecutionEnvironment 流执行环境
     */
    private StreamExecutionEnvironment createStreamExecutionEnvironment(Configuration config) {
        LOG.info("创建流执行环境...");
        
        // 创建执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        
        // 设置默认并行度
        env.setParallelism(1);
        
        // 应用文件系统配置
        if (config != null) {
            String checkpointsDir = config.getString("state.checkpoints.dir", null);
            if (checkpointsDir != null) {
                LOG.info("Checkpoints 目录：{}", checkpointsDir);
            }
            
            String savepointsDir = config.getString("state.savepoints.dir", null);
            if (savepointsDir != null) {
                LOG.info("Savepoints 目录：{}", savepointsDir);
            }
        }
        
        LOG.info("流执行环境创建成功");
        return env;
    }
    
    /**
     * 运行基础工作流
     * 
     * @param env 流执行环境
     * @throws Exception 工作流执行异常
     */
    private void runBasicWorkflow(StreamExecutionEnvironment env) throws Exception {
        LOG.info("启动基础工作流...");
        BasicWorkflow workflow = new BasicWorkflow();
        workflow.runWorkflow(env);
    }
    
    /**
     * 运行 Kafka 流处理工作流
     * 
     * @param env 流执行环境
     * @param args 命令行参数
     * @throws Exception 工作流执行异常
     */
    private void runKafkaWorkflow(StreamExecutionEnvironment env, String[] args) throws Exception {
        LOG.info("启动 Kafka 流处理工作流...");
        KafkaStreamWorkflow kafkaWorkflow = new KafkaStreamWorkflow();
        
        String kafkaServers = args.length > 1 ? args[1] : "localhost:9092";
        String inputTopic = args.length > 2 ? args[2] : "orders";
        String outputTopic = args.length > 3 ? args[3] : "order-aggregates";
        
        kafkaWorkflow.runKafkaWorkflow(env, kafkaServers, inputTopic, outputTopic);
    }
    
    /**
     * 运行 Elasticsearch 流处理工作流
     * 
     * @param env 流执行环境
     * @param args 命令行参数
     * @throws Exception 工作流执行异常
     */
    private void runEsWorkflow(StreamExecutionEnvironment env, String[] args) throws Exception {
        LOG.info("启动 Elasticsearch 流处理工作流...");
        EsStreamWorkflow esWorkflow = new EsStreamWorkflow();
        
        String esHosts = System.getenv("ES_HOSTS");
        if (esHosts == null || esHosts.isEmpty()) {
            esHosts = args.length > 1 ? args[1] : "http://localhost:9200";
        }
        
        esWorkflow.runEsWorkflow(env, esHosts);
    }
    
    /**
     * 运行完整业务工作流 (数据库→Kafka→Flink(ES)→数据库)
     * 
     * @param env 流执行环境
     * @param args 命令行参数
     * @throws Exception 工作流执行异常
     */
    private void runBusinessWorkflow(StreamExecutionEnvironment env, String[] args) throws Exception {
        LOG.info("启动完整业务工作流 (数据库→Kafka→Flink(ES)→数据库)...");
        BusinessDataWorkflow businessWorkflow = new BusinessDataWorkflow();
        
        String dbUrl = getEnvOrArg("DB_URL", args, 3, "jdbc:mysql://localhost:3306/flink_db?useSSL=false&serverTimezone=UTC");
        String dbUser = getEnvOrArg("DB_USER", args, 4, "root");
        String dbPassword = getEnvOrArg("DB_PASSWORD", args, 5, "password");
        String kafkaServers = getEnvOrArg("KAFKA_SERVERS", args, 6, "localhost:9092");
        String esHosts = getEnvOrArg("ES_HOSTS", args, 7, "http://localhost:9200");
        String inputTopic = args.length > 1 ? args[1] : "business-orders";
        String outputTopic = args.length > 2 ? args[2] : "business-results";
        
        businessWorkflow.runBusinessWorkflow(
            env,
            dbUrl, dbUser, dbPassword,
            kafkaServers, esHosts,
            inputTopic, outputTopic
        );
    }
    
    /**
     * 运行 JDBC 数据库示例
     * 
     * @param args 命令行参数
     * @throws Exception 数据库操作异常
     */
    private void runJdbcExample(String[] args) throws Exception {
        LOG.info("启动 JDBC 数据库示例...");
        String dbUrl = args.length > 1 ? args[1] : "jdbc:mysql://localhost:3306/flink_db?useSSL=false&serverTimezone=UTC";
        String dbUser = args.length > 2 ? args[2] : "root";
        String dbPassword = args.length > 3 ? args[3] : "password";
        
        com.example.project.core.jdbc.JdbcExample jdbcExample = new com.example.project.core.jdbc.JdbcExample();
        LOG.warn("JDBC 示例使用默认配置，请在代码中修改数据库连接参数");
        jdbcExample.runExample();
    }
    
    /**
     * 显示配置信息
     * 
     * @param flinkConfig Flink 配置对象
     */
    private void showConfigInfo(Configuration flinkConfig) {
        LOG.info("文件系统配置已加载:");
        LOG.info("  - Checkpoints 目录：{}", flinkConfig.getString("state.checkpoints.dir", "未设置"));
        LOG.info("  - Savepoints 目录：{}", flinkConfig.getString("state.savepoints.dir", "未设置"));
        LOG.info("  - S3A Endpoint: {}", flinkConfig.getString("fs.s3a.endpoint", "未设置"));
        LOG.info("  - S3A Path Style Access: {}", flinkConfig.getBoolean("fs.s3a.path.style.access", false));
        printUsage();
    }
    
    /**
     * 打印使用说明
     */
    private void printUsage() {
        LOG.info("使用方法:");
        LOG.info("  无参数或 'basic' - 运行基础工作流");
        LOG.info("  'kafka [kafkaServers] [inputTopic] [outputTopic]' - 运行 Kafka 工作流");
        LOG.info("  'es [esHosts]' - 运行 Elasticsearch 工作流 (优先使用环境变量 ES_HOSTS)");
        LOG.info("  'business [inputTopic] [outputTopic] [dbUrl] [dbUser] [dbPassword] [kafkaServers] [esHosts]' - 运行完整业务工作流");
        LOG.info("  'jdbc [dbUrl] [dbUser] [dbPassword]' - 运行 JDBC 示例");
        LOG.info("  'config' - 显示文件系统配置");
    }
    
    /**
     * 获取环境变量或命令行参数的值
     * 
     * @param envName 环境变量名
     * @param args 命令行参数数组
     * @param argIndex 参数索引位置
     * @param defaultValue 默认值
     * @return 环境变量值、参数值或默认值
     */
    private String getEnvOrArg(String envName, String[] args, int argIndex, String defaultValue) {
        String value = System.getenv(envName);
        if (value == null || value.isEmpty()) {
            value = args.length > argIndex ? args[argIndex] : defaultValue;
        }
        return value;
    }
}
