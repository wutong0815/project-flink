package com.example.project.flinktool.utils;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.state.hashmap.HashMapStateBackend;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.streaming.api.functions.source.SourceFunction;

import java.util.Random;
import java.util.concurrent.TimeUnit;

/**
 * 带有Savepoint功能的示例Flink流处理应用
 * 该应用从源生成数据，并定期创建检查点和savepoint
 */
public class SavepointExample {

    public static void main(String[] args) throws Exception {
        // 设置执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        
        // 从命令行参数获取savepoint路径
        String savepointPath = null;
        if (args.length > 0) {
            savepointPath = args[0];
        }
        
        // 如果提供了savepoint路径，则从该路径恢复作业
        if (savepointPath != null && !savepointPath.isEmpty()) {
            System.out.println("尝试从savepoint恢复: " + savepointPath);
            env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(new Configuration());
            env.getConfig().setRestartStrategy(RestartStrategies.noRestart());
        }

        // 设置状态后端为HashMapStateBackend，配合S3A文件系统
        env.setStateBackend(new HashMapStateBackend());
        
        // 配置检查点到MinIO
        env.getCheckpointConfig().setCheckpointStorage("s3a://flink-checkpoints/");

        // 开启检查点
        env.enableCheckpointing(5000); // 每5秒做一次checkpoint

        // 设置检查点配置
        CheckpointConfig checkpointConfig = env.getCheckpointConfig();
        checkpointConfig.setCheckpointInterval(5000);
        checkpointConfig.setCheckpointTimeout(30000);
        checkpointConfig.setMaxConcurrentCheckpoints(1);
        checkpointConfig.enableExternalizedCheckpoints(
            CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);
        
        // 设置重启策略
        env.setRestartStrategy(RestartStrategies.fixedDelayRestart(
                3, // 尝试重启次数
                Time.of(10, TimeUnit.SECONDS) // 重启间隔
        ));

        // 添加一个简单的数据源
        DataStream<String> source = env.addSource(new SimpleStringGenerator());

        // 处理数据
        DataStream<Tuple2<String, Integer>> processedData = source
                .map(new MapFunction<String, Tuple2<String, Integer>>() {
                    @Override
                    public Tuple2<String, Integer> map(String value) {
                        return new Tuple2<>(value, 1);
                    }
                });

        // 输出结果到控制台
        processedData.addSink(new SinkFunction<Tuple2<String, Integer>>() {
            @Override
            public void invoke(Tuple2<String, Integer> value, Context context) {
                System.out.println(value.f0 + ": " + value.f1);
            }
        });

        // 执行程序
        env.execute("Savepoint Example Job");
    }

    /**
     * 简单的字符串生成源函数
     */
    public static class SimpleStringGenerator implements SourceFunction<String> {
        private volatile boolean isRunning = true;
        private Random random = new Random();

        @Override
        public void run(SourceContext<String> ctx) throws Exception {
            int counter = 0;
            while (isRunning) {
                // 生成递增的数据
                String data = "event-" + counter++;
                ctx.collect(data);
                
                // 每秒生成一条数据
                Thread.sleep(1000);
            }
        }

        @Override
        public void cancel() {
            isRunning = false;
        }
    }
}