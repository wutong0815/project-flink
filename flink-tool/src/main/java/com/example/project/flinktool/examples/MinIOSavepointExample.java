package com.example.project.flinktool.examples;

import com.example.project.flinktool.utils.SavepointUtil;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.client.program.JobClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * MinIO Savepoint使用示例
 * 展示如何将Flink savepoint保存到MinIO存储
 */
public class MinIOSavepointExample {
    
    private static final Logger LOG = LoggerFactory.getLogger(MinIOSavepointExample.class);
    
    public static void main(String[] args) {
        try {
            // 示例：触发savepoint到MinIO
            exampleTriggerSavepointToMinIO();
            
            // 示例：构建MinIO路径
            exampleBuildMinIOPath();
            
            // 示例：使用JobClient触发savepoint
            exampleTriggerSavepointWithJobClient();
            
        } catch (Exception e) {
            LOG.error("执行示例时发生错误", e);
        }
    }
    
    /**
     * 示例：触发savepoint到MinIO存储
     */
    public static void exampleTriggerSavepointToMinIO() throws Exception {
        // Flink作业ID（需要替换为实际的作业ID）
        String jobID = "123e4567-e89b-12d3-a456-426614174000";
        
        // MinIO配置
        String bucketName = "flink-savepoints";  // MinIO桶名称
        String basePath = "production";          // 基础路径
        String jobName = "word-count-job";       // 作业名称
        
        // 获取Flink配置
        Configuration flinkConfig = SavepointUtil.getDefaultFlinkConfiguration();
        
        LOG.info("开始触发savepoint到MinIO...");
        LOG.info("作业ID: {}", jobID);
        LOG.info("MinIO桶: {}", bucketName);
        LOG.info("基础路径: {}", basePath);
        LOG.info("作业名称: {}", jobName);
        
        // 触发savepoint到MinIO
        String savepointLocation = SavepointUtil.triggerSavepointToMinIO(
            jobID, bucketName, basePath, jobName, flinkConfig);
        
        LOG.info("Savepoint成功创建于: {}", savepointLocation);
    }
    
    /**
     * 示例：单独构建MinIO路径
     */
    public static void exampleBuildMinIOPath() {
        String bucketName = "flink-backups";
        String basePath = "savepoints";
        String jobName = "streaming-etl";
        
        String minioPath = SavepointUtil.buildMinIOSavepointPath(bucketName, basePath, jobName);
        LOG.info("构建的MinIO路径: {}", minioPath);
        
        // 不带作业名称的路径
        String simplePath = SavepointUtil.buildMinIOSavepointPath(bucketName, basePath, null);
        LOG.info("简化路径: {}", simplePath);
    }
    
    /**
     * 使用现有triggerSavepoint方法配合MinIO路径
     */
    public static void exampleUsingExistingMethod() throws Exception {
        String jobID = "abcdef12-3456-7890-abcd-ef1234567890";
        Configuration flinkConfig = SavepointUtil.getDefaultFlinkConfiguration();
        
        // 直接构建MinIO路径并使用现有方法
        String savepointPath = SavepointUtil.buildMinIOSavepointPath(
            "my-bucket", "flink-checkpoints", "my-streaming-job");
        
        String result = SavepointUtil.triggerSavepoint(jobID, savepointPath, flinkConfig);
        LOG.info("使用现有方法的结果: {}", result);
    }
    
    /**
     * 示例：使用JobClient触发savepoint到MinIO（适用于流处理作业）
     * 正确的使用方式：先启动作业，再获取JobClient触发savepoint
     */
    public static void exampleTriggerSavepointWithJobClient() throws Exception {
        LOG.info("开始演示使用JobClient触发savepoint...");
        
        // 创建流处理环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        
        // 配置检查点
        env.enableCheckpointing(5000);
        env.getCheckpointConfig().setCheckpointStorage("s3a://flink-checkpoints/");
        
        // 创建一个简单的流处理作业
        env.fromElements("Hello", "World", "Flink")
           .map(String::toUpperCase)
           .print();
        
        // 启动作业并获取JobClient
        JobClient jobClient = env.executeAsync("Test Job for Savepoint");
        
        // 等待作业稳定运行
        Thread.sleep(5000);
        
        try {
            // MinIO配置
            String bucketName = "flink-streaming-savepoints";
            String basePath = "realtime-jobs";
            String jobName = "sensor-data-processing";
            
            LOG.info("配置信息:");
            LOG.info("- MinIO桶: {}", bucketName);
            LOG.info("- 基础路径: {}", basePath);
            LOG.info("- 作业名称: {}", jobName);
            
            // 使用JobClient触发savepoint到MinIO
            String savepointLocation = SavepointUtil.triggerSavepointToMinIOWithJobClient(
                jobClient, bucketName, basePath, jobName);
            
            LOG.info("JobClient方式savepoint成功创建于: {}", savepointLocation);
        } finally {
            // 清理资源
            jobClient.cancel().get();
            LOG.info("作业已取消");
        }
    }
    
    /**
     * 示例：使用JobClient触发savepoint到指定路径
     */
    public static void exampleTriggerSavepointWithPath() throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        
        // 配置检查点
        env.enableCheckpointing(5000);
        env.getCheckpointConfig().setCheckpointStorage("s3a://flink-checkpoints/");
        
        // 创建作业
        env.fromElements(1, 2, 3, 4, 5)
           .map(x -> x * 2)
           .print();
        
        // 启动作业
        JobClient jobClient = env.executeAsync("Path Test Job");
        Thread.sleep(3000);
        
        try {
            String savepointPath = "s3://my-bucket/custom-path/savepoint-" + System.currentTimeMillis();
            String result = SavepointUtil.triggerSavepointWithJobClient(jobClient, savepointPath);
            LOG.info("指定路径的savepoint结果: {}", result);
        } finally {
            jobClient.cancel().get();
        }
    }
}