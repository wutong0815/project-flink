package com.example.project.flinktool.utils;

import org.apache.flink.api.common.JobID;
import org.apache.flink.client.program.ClusterClient;
import org.apache.flink.client.program.rest.RestClusterClient;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.execution.SavepointFormatType;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.client.program.JobClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Savepoint操作工具类
 * 提供创建、恢复和管理savepoint的功能
 */
public class SavepointUtil {
    
    private static final Logger LOG = LoggerFactory.getLogger(SavepointUtil.class);

    /**
     * 触发savepoint
     *
     * @param jobIDStr 作业ID
     * @param savepointPath savepoint保存路径
     * @param flinkConfig Flink配置
     * @return savepoint路径
     * @throws Exception
     */
    public static String triggerSavepoint(String jobIDStr, String savepointPath, Configuration flinkConfig) throws Exception {
        JobID jobID = JobID.fromHexString(jobIDStr);
        
        try (ClusterClient<?> client = new RestClusterClient<>(flinkConfig, "localhost")) {
            LOG.info("Triggering savepoint for job {} at {}", jobID, savepointPath);
            // 触发savepoint
            String savepointLocation = client.triggerSavepoint(jobID, savepointPath, SavepointFormatType.CANONICAL).get();
            LOG.info("Successfully created savepoint at {}", savepointLocation);
            return savepointLocation;
        }
    }

    /**
     * 取消作业并创建savepoint
     *
     * @param jobIDStr 作业ID
     * @param savepointPath savepoint保存路径
     * @param flinkConfig Flink配置
     * @return savepoint路径
     * @throws Exception
     */
    public static String stopWithSavepoint(String jobIDStr, String savepointPath, Configuration flinkConfig) throws Exception {
        JobID jobID = JobID.fromHexString(jobIDStr);
        
        try (ClusterClient<?> client = new RestClusterClient<>(flinkConfig, "localhost")) {
            LOG.info("Stopping job {} with savepoint at {}", jobID, savepointPath);
            // 停止作业并创建savepoint
            String savepointLocation = client.stopWithSavepoint(jobID, true, savepointPath, SavepointFormatType.CANONICAL).get();
            LOG.info("Successfully stopped job {} with savepoint at {}", jobID, savepointLocation);
            return savepointLocation;
        }
    }

    /**
     * 从savepoint恢复作业
     *
     * @param jarPath JAR文件路径
     * @param savepointPath savepoint路径
     * @param className 主类名
     * @param programArgs 程序参数
     * @param flinkConfig Flink配置
     * @throws Exception
     */
    public static void restoreFromSavepoint(String jarPath, String savepointPath, String className, 
                                          String[] programArgs, Configuration flinkConfig) throws Exception {
        // 注意：实际的作业提交需要通过Flink CLI或REST API
        // 这里提供一个示例框架
        LOG.info("准备从savepoint {} 恢复作业", savepointPath);
        LOG.info("JAR路径: {}", jarPath);
        LOG.info("主类: {}", className);
        LOG.info("程序参数: {}", String.join(" ", programArgs));
        
        // 实际实现应该使用Flink REST API提交作业
        // 参考: https://nightlies.apache.org/flink/flink-docs-release-1.18/docs/ops/rest_api/
        throw new UnsupportedOperationException("请使用Flink REST API提交作业");
    }
    
    /**
     * 构建MinIO上的savepoint路径
     * 
     * @param bucketName MinIO桶名称
     * @param basePath 基础路径
     * @param jobName 作业名称（可选）
     * @return 完整的MinIO路径
     */
    public static String buildMinIOSavepointPath(String bucketName, String basePath, String jobName) {
        StringBuilder path = new StringBuilder();
        path.append("s3://")
            .append(bucketName)
            .append("/")
            .append(basePath);
        
        if (jobName != null && !jobName.isEmpty()) {
            path.append("/").append(jobName);
        }
        
        // 添加时间戳确保唯一性
        String timestamp = String.valueOf(System.currentTimeMillis());
        path.append("/savepoint-").append(timestamp);
        
        LOG.info("构建的MinIO savepoint路径: {}", path.toString());
        return path.toString();
    }
    
    /**
     * 触发savepoint到MinIO存储
     * 
     * @param jobIDStr 作业ID
     * @param bucketName MinIO桶名称
     * @param basePath 基础路径
     * @param jobName 作业名称
     * @param flinkConfig Flink配置
     * @return savepoint在MinIO上的完整路径
     * @throws Exception
     */
    public static String triggerSavepointToMinIO(String jobIDStr, String bucketName, String basePath, 
                                               String jobName, Configuration flinkConfig) throws Exception {
        String savepointPath = buildMinIOSavepointPath(bucketName, basePath, jobName);
        return triggerSavepoint(jobIDStr, savepointPath, flinkConfig);
    }
    
    /**
     * 获取默认的Flink配置，用于连接到本地运行的Flink集群
     * 
     * @return Flink配置对象
     */
    public static Configuration getDefaultFlinkConfiguration() {
        Configuration flinkConfig = new Configuration();
        flinkConfig.setString("rest.address", "localhost");
        flinkConfig.setInteger("rest.port", 8081);
        
        // 配置Hadoop S3A参数用于访问MinIO
        flinkConfig.setString("s3.endpoint", "http://minio:9000");
        flinkConfig.setString("s3.access-key", "minioadmin");
        flinkConfig.setString("s3.secret-key", "minioadmin");
        flinkConfig.setBoolean("s3.path.style.access", true);
        flinkConfig.setBoolean("s3.connection.ssl.enabled", false);
        
        return flinkConfig;
    }
    
    /**
     * 使用JobClient触发savepoint（适用于流处理作业）
     * 
     * @param env StreamExecutionEnvironment环境
     * @param savepointPath savepoint保存路径
     * @return savepoint路径
     * @throws Exception
     */
    public static String triggerSavepointWithJobClient(StreamExecutionEnvironment env, String savepointPath) throws Exception {
        LOG.info("使用JobClient触发savepoint到路径: {}", savepointPath);
        
        // 执行环境并获取JobClient
        JobClient jobClient = env.executeAsync("Savepoint Trigger Job");
        
        try {
            // 使用JobClient触发savepoint
            String savepointLocation = jobClient.triggerSavepoint(savepointPath).get();
            LOG.info("JobClient成功创建savepoint于: {}", savepointLocation);
            return savepointLocation;
        } finally {
            // 取消作业以释放资源
            jobClient.cancel().get();
            LOG.info("作业已取消");
        }
    }
    
    /**
     * 使用JobClient触发savepoint到MinIO（适用于流处理作业）
     * 
     * @param env StreamExecutionEnvironment环境
     * @param bucketName MinIO桶名称
     * @param basePath 基础路径
     * @param jobName 作业名称
     * @return savepoint在MinIO上的完整路径
     * @throws Exception
     */
    public static String triggerSavepointToMinIOWithJobClient(StreamExecutionEnvironment env, 
                                                            String bucketName, String basePath, String jobName) throws Exception {
        String savepointPath = buildMinIOSavepointPath(bucketName, basePath, jobName);
        return triggerSavepointWithJobClient(env, savepointPath);
    }
    
    /**
     * 使用JobClient触发savepoint到MinIO（适用于流处理作业）
     * 注意：此方法适用于正在运行的作业，需要先启动作业再触发savepoint
     * 
     * @param jobClient 已运行作业的JobClient
     * @param bucketName MinIO桶名称
     * @param basePath 基础路径
     * @param jobName 作业名称
     * @return savepoint在MinIO上的完整路径
     * @throws Exception
     */
    public static String triggerSavepointToMinIOWithJobClient(JobClient jobClient, 
                                                            String bucketName, String basePath, String jobName) throws Exception {
        String savepointPath = buildMinIOSavepointPath(bucketName, basePath, jobName);
        return triggerSavepointWithJobClient(jobClient, savepointPath);
    }
    
    /**
     * 通过作业ID获取JobClient（适用于已有运行中的作业）
     * 
     * @param jobIDStr 作业ID字符串
     * @param flinkConfig Flink配置
     * @return JobClient实例
     * @throws Exception
     */
    public static JobClient getJobClientByJobID(String jobIDStr, Configuration flinkConfig) throws Exception {
        JobID jobID = JobID.fromHexString(jobIDStr);
        ClusterClient<?> client = new RestClusterClient<>(flinkConfig, "localhost");
        
        // 注意：这种方法在Flink 1.18中可能需要调整
        // 建议直接使用Flink REST API获取作业状态和触发savepoint
        throw new UnsupportedOperationException("请使用Flink REST API直接操作作业");
    }
    
    /**
     * 使用REST API触发savepoint（推荐方式）
     * 这是Flink 1.18+版本推荐的方式
     * 
     * @param jobIDStr 作业ID
     * @param savepointPath savepoint保存路径
     * @param flinkConfig Flink配置
     * @return savepoint路径
     * @throws Exception
     */
    public static String triggerSavepointViaRestAPI(String jobIDStr, String savepointPath, Configuration flinkConfig) throws Exception {
        // 这是Flink 1.18+版本的标准做法
        return triggerSavepoint(jobIDStr, savepointPath, flinkConfig);
    }
}