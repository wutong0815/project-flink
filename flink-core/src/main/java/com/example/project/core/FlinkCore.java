package com.example.project.core;

import com.example.project.core.minio.MinioClientUtil;
import com.example.project.core.minio.MinioExample;
import com.example.project.core.kafka.KafkaClientUtil;
import com.example.project.core.kafka.KafkaExample;
import com.example.project.core.akka.AkkaClientUtil;
import com.example.project.core.akka.AkkaExample;
import com.example.project.core.es.EsClientUtil;
import com.example.project.core.es.EsExample;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

/**
 * Flink Core 应用入口
 * 
 * 集成了 MinIO、Kafka、Akka 和 Elasticsearch 客户端功能:
 * - 无参数或 'example': 运行 MinIO 示例
 * - 'minio': 初始化 MinIO 客户端 (不执行操作)
 * - 'kafka': 初始化 Kafka 客户端并测试
 * - 'kafka-example': 运行 Kafka 示例
 * - 'akka': 初始化 Akka 客户端 (不执行操作)
 * - 'akka-example': 运行 Akka 示例
 * - 'es': 初始化 Elasticsearch 客户端 (不执行操作)
 * - 'es-example': 运行 Elasticsearch 示例
 */
@SpringBootApplication
public class FlinkCore implements CommandLineRunner {
    
    private static final Logger LOG = LoggerFactory.getLogger(FlinkCore.class);
    
    public static void main(String[] args) {
        SpringApplication.run(FlinkCore.class, args);
    }
    
    @Override
    public void run(String... args) throws Exception {
        if (args.length == 0 || "example".equals(args[0])) {
            // 运行 MinIO 示例
            LOG.info("启动 MinIO 示例...");
            MinioExample example = new MinioExample();
            example.runExample();
        } else if ("minio".equals(args[0])) {
            // 仅初始化 MinIO 客户端
            LOG.info("初始化 MinIO 客户端...");
            MinioClientUtil minioClient = null;
            try {
                minioClient = new MinioClientUtil(
                        "http://localhost:9000",
                        "minioadmin",
                        "minioadmin",
                        false);
                LOG.info("MinIO 客户端初始化成功");
                // 可以在这里添加自定义逻辑
            } finally {
                if (minioClient != null) {
                    minioClient.closeClient();
                }
            }
        } else if ("kafka-example".equals(args[0])) {
            // 运行 Kafka 示例
            LOG.info("启动 Kafka 示例...");
            KafkaExample example = new KafkaExample();
            example.runExample();
        } else if ("kafka".equals(args[0])) {
            // 初始化 Kafka 客户端
            LOG.info("初始化 Kafka 客户端...");
            String kafkaServers = args.length > 1 ? args[1] : "localhost:9092";
            String groupId = args.length > 2 ? args[2] : "flink-core-group";
            
            KafkaClientUtil kafkaClient = null;
            try {
                kafkaClient = new KafkaClientUtil(kafkaServers, groupId);
                LOG.info("Kafka 客户端初始化成功");
                
                // 验证配置
                if (kafkaClient.isValidConfig()) {
                    LOG.info("Kafka 配置验证通过");
                    
                    // 创建 Source 和 Sink 示例
                    String testTopic = args.length > 3 ? args[3] : "test-topic";
                    LOG.info("创建 Kafka Source - Topic: {}", testTopic);
                    var source = kafkaClient.createSource(testTopic);
                    LOG.info("Kafka Source 创建成功");
                    
                    LOG.info("创建 Kafka Sink - Topic: {}", testTopic);
                    var sink = kafkaClient.createSink(testTopic);
                    LOG.info("Kafka Sink 创建成功");
                }
            } catch (Exception e) {
                LOG.error("Kafka 客户端初始化失败", e);
                throw e;
            }
        } else if ("akka-example".equals(args[0])) {
            // 运行 Akka 示例
            LOG.info("启动 Akka 示例...");
            AkkaExample example = new AkkaExample();
            example.runFullExample();
        } else if ("akka".equals(args[0])) {
            // 初始化 Akka 客户端
            LOG.info("初始化 Akka 客户端...");
            String systemName = args.length > 1 ? args[1] : "flink-core-system";
            
            AkkaClientUtil akkaClient = null;
            try {
                akkaClient = new AkkaClientUtil(systemName);
                LOG.info("Akka 客户端初始化成功");
                
                // 验证 ActorSystem 是否运行
                if (akkaClient.isRunning()) {
                    LOG.info("ActorSystem 正在运行");
                    LOG.info("可以使用 createActor() 方法创建 Actor");
                }
            } finally {
                if (akkaClient != null) {
                    akkaClient.shutdown();
                }
            }
        } else if ("es-example".equals(args[0])) {
            // 运行 Elasticsearch 示例
            LOG.info("启动 Elasticsearch 示例...");
            EsExample example = new EsExample("http://localhost:9200");
            example.runExample();
        } else if ("es".equals(args[0])) {
            // 初始化 Elasticsearch 客户端
            LOG.info("初始化 Elasticsearch 客户端...");
            String esHosts = args.length > 1 ? args[1] : "http://localhost:9200";
            String username = args.length > 2 ? args[2] : null;
            String password = args.length > 3 ? args[3] : null;
            
            EsClientUtil esClient = null;
            try {
                if (username != null && !username.isEmpty() && password != null && !password.isEmpty()) {
                    esClient = new EsClientUtil(esHosts, username, password);
                } else {
                    esClient = new EsClientUtil(esHosts);
                }
                LOG.info("Elasticsearch 客户端初始化成功");
                
                // 验证配置
                if (esClient.isValidConfig()) {
                    LOG.info("Elasticsearch 配置验证通过");
                    LOG.info("可以使用 createIndex(), addDocument() 等方法");
                }
            } catch (Exception e) {
                LOG.error("Elasticsearch 客户端初始化失败", e);
                throw e;
            } finally {
                if (esClient != null) {
                    esClient.closeClient();
                }
            }
        } else {
            LOG.error("未知的命令：{}", args[0]);
            LOG.info("使用方法:");
            LOG.info("  无参数或 'example' - 运行 MinIO 完整示例");
            LOG.info("  'minio' - 初始化 MinIO 客户端");
            LOG.info("  'kafka [kafkaServers] [groupId] [topic]' - 初始化 Kafka 客户端");
            LOG.info("  'kafka-example' - 运行 Kafka 示例");
            LOG.info("  'akka [systemName]' - 初始化 Akka 客户端");
            LOG.info("  'akka-example' - 运行 Akka 示例");
            LOG.info("  'es [hosts] [username] [password]' - 初始化 Elasticsearch 客户端");
            LOG.info("  'es-example' - 运行 Elasticsearch 示例");
        }
    }
}
