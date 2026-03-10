package com.example.project.core.akka;

import akka.NotUsed;
import akka.actor.ActorSystem;
import akka.stream.*;
import akka.stream.javadsl.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.CompletionStage;

/**
 * Akka Stream 示例
 * 
 * 演示了如何使用 Akka Stream 进行流式数据处理
 */
public class ExampleStream {
    
    private static final Logger LOG = LoggerFactory.getLogger(ExampleStream.class);
    
    private final ActorSystem system;
    private final Materializer materializer;
    
    public ExampleStream() {
        this.system = ActorSystem.create("example-stream-system");
        this.materializer = SystemMaterializer.get(system).materializer();
    }
    
    /**
     * 运行简单的流处理示例
     */
    public void runSimpleExample() {
        LOG.info("运行 Akka Stream 简单示例...");
        
        // 创建一个简单的流：Source -> Flow -> Sink
        Source<Integer, NotUsed> source = Source.from(Arrays.asList(1, 2, 3, 4, 5));
        
        Flow<Integer, Integer, NotUsed> flow = Flow.<Integer>create()
            .map(x -> x * 2)
            .filter(x -> x > 4);
        
        Sink<Integer, CompletionStage<Integer>> sink = Sink.fold(0, (acc, x) -> acc + x);
        
        // 连接流并运行
        CompletionStage<Integer> result = source.via(flow).runWith(sink, materializer);
        
        result.thenAccept(sum -> {
            LOG.info("计算结果：{}", sum);
            LOG.info("示例执行完成");
        });
    }
    
    /**
     * 运行带有背压的流处理示例
     */
    public void runBackpressureExample() {
        LOG.info("运行背压示例...");
        
        Source<Integer, NotUsed> source = Source.range(1, 100);
        
        Flow<Integer, Integer, NotUsed> flow = Flow.<Integer>create()
            .mapAsync(4, x -> {
                // 模拟异步操作
                try {
                    Thread.sleep(10);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
                return java.util.concurrent.CompletableFuture.completedFuture(x * 2);
            })
            .buffer(10, OverflowStrategy.backpressure());
        
        Sink<Integer, CompletionStage<List<Integer>>> sink = Sink.seq();
        
        CompletionStage<List<Integer>> result = source.via(flow).runWith(sink, materializer);
        
        result.thenAccept(list -> {
            LOG.info("处理了 {} 个元素", list.size());
            LOG.info("前 10 个结果：{}", list.subList(0, Math.min(10, list.size())));
        });
    }
    
    /**
     * 运行窗口聚合示例
     */
    public void runWindowingExample() {
        LOG.info("运行窗口聚合示例...");
        
        Source<String, NotUsed> source = Source.from(Arrays.asList(
            "apple", "banana", "cherry", "date", "elderberry"
        ));
        
        Flow<String, String, NotUsed> flow = Flow.<String>create()
            .groupedWithin(3, Duration.ofSeconds(1))
            .map(list -> String.join(", ", list));
        
        Sink<String, CompletionStage<String>> sink = Sink.reduce((a, b) -> a + " | " + b);
        
        CompletionStage<String> result = source.via(flow).runWith(sink, materializer);
        
        result.thenAccept(grouped -> {
            LOG.info("分组结果：{}", grouped);
        });
    }
    
    /**
     * 关闭系统资源
     */
    public void shutdown() {
        LOG.info("关闭 ActorSystem...");
        system.terminate();
    }
}
