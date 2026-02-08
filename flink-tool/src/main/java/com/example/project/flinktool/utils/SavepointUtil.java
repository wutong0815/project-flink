package com.example.project.flinktool.utils;

import org.apache.flink.api.common.JobID;
import org.apache.flink.client.program.ClusterClient;
import org.apache.flink.client.program.rest.RestClusterClient;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.execution.SavepointFormatType;
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
        try (ClusterClient<?> client = new RestClusterClient<>(flinkConfig, "localhost")) {
            LOG.info("Restoring job from savepoint {}", savepointPath);
            // 从savepoint提交作业
            client.run(jarPath, className, programArgs, true, savepointPath);
            LOG.info("Successfully submitted job from savepoint {}", savepointPath);
        }
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
}