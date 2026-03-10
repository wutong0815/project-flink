package com.example.project.base.workflow;

import com.example.project.base.model.UserEvent;
import com.example.project.base.util.FileSystemConfigLoader;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.connector.source.util.ratelimit.RateLimiterStrategy;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.datagen.source.DataGeneratorSource;
import org.apache.flink.connector.datagen.source.GeneratorFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Flink 基础工作流示例
 * 
 * 演示了:
 * 1. 使用 DataGenerator 生成测试数据
 * 2. Map/Filter转换处理数据
 * 3. KeyBy分组聚合
 */
public class BasicWorkflow {
    
    private static final Logger LOG = LoggerFactory.getLogger(BasicWorkflow.class);
    
    /**
     * 运行基础工作流
     */
    public void runWorkflow() throws Exception {
        runWorkflow(null);
    }
    
    /**
     * 运行基础工作流（带配置）
     * 
     * @param config Flink 配置对象（可选）
     */
    public void runWorkflow(Configuration config) throws Exception {
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
        
        // 2. 生成测试数据流
        DataGeneratorSource<UserEvent> generatorSource = new DataGeneratorSource<>(
                value -> {
                    long userId = value % 5; // 5 个用户
                    String[] eventTypes = {"LOGIN", "LOGOUT", "PURCHASE", "BROWSE"};
                    String eventType = eventTypes[(int)(value % 4)];
                    return new UserEvent(userId, eventType, System.currentTimeMillis());
                },
                Long.MAX_VALUE,
                TypeInformation.of(UserEvent.class)
        );
        
        DataStream<UserEvent> sourceStream = env.fromSource(
                generatorSource,
                WatermarkStrategy.noWatermarks(),
                "DataGeneratorSource"
        );
        
        // 3. 数据处理流程：过滤 -> 映射 -> 聚合
        sourceStream
                .filter(event -> "PURCHASE".equals(event.eventType) || "LOGIN".equals(event.eventType))
                .map(new MapFunction<UserEvent, Tuple2<Long, Integer>>() {
                    @Override
                    public Tuple2<Long, Integer> map(UserEvent event) {
                        return Tuple2.of(event.userId, 1);
                    }
                }, Types.TUPLE(Types.LONG, Types.INT))
                .keyBy(value -> value.f0)
                .sum(1)
                .print("用户事件统计");
        
        // 4. 执行作业
        LOG.info("开始执行 Flink 基础工作流...");
        env.execute("Basic Flink Workflow");
    }
    
    /**
     * 主方法 - 用于独立运行
     */
    public static void main(String[] args) throws Exception {
        BasicWorkflow workflow = new BasicWorkflow();
        workflow.runWorkflow();
    }
}
