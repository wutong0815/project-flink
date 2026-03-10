package com.example.project.base;

import com.example.project.base.workflow.BasicWorkflow;
import com.example.project.base.workflow.KafkaStreamWorkflow;
import com.example.project.base.util.FileSystemConfigLoader;
import org.apache.flink.configuration.Configuration;
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
        
        if (args.length == 0 || "basic".equals(args[0])) {
            // 运行基础工作流
            LOG.info("启动基础工作流...");
            BasicWorkflow workflow = new BasicWorkflow();
            workflow.runWorkflow();
        } else if ("kafka".equals(args[0])) {
            // 运行 Kafka 工作流
            LOG.info("启动 Kafka 流处理工作流...");
            KafkaStreamWorkflow kafkaWorkflow = new KafkaStreamWorkflow();
            
            String kafkaServers = args.length > 1 ? args[1] : "localhost:9092";
            String inputTopic = args.length > 2 ? args[2] : "orders";
            String outputTopic = args.length > 3 ? args[3] : "order-aggregates";
            
            kafkaWorkflow.runKafkaWorkflow(kafkaServers, inputTopic, outputTopic);
        } else if ("config".equals(args[0])) {
            // 仅显示配置信息，不执行工作流
            LOG.info("文件系统配置已加载:");
            LOG.info("  - Checkpoints 目录：{}", flinkConfig.getString("state.checkpoints.dir", "未设置"));
            LOG.info("  - Savepoints 目录：{}", flinkConfig.getString("state.savepoints.dir", "未设置"));
            LOG.info("  - S3A Endpoint: {}", flinkConfig.getString("fs.s3a.endpoint", "未设置"));
            LOG.info("  - S3A Path Style Access: {}", flinkConfig.getBoolean("fs.s3a.path.style.access", false));
            LOG.info("使用方法:");
            LOG.info("  'basic' - 运行基础工作流");
            LOG.info("  'kafka [kafkaServers] [inputTopic] [outputTopic]' - 运行 Kafka 工作流");
            LOG.info("  'config' - 显示当前配置");
        } else {
            LOG.error("未知的工作流类型：{}", args[0]);
            LOG.info("使用方法:");
            LOG.info("  无参数或 'basic' - 运行基础工作流");
            LOG.info("  'kafka [kafkaServers] [inputTopic] [outputTopic]' - 运行 Kafka 工作流");
            LOG.info("  'config' - 显示文件系统配置");
        }
    }
}
