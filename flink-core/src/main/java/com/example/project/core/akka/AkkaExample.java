package com.example.project.core.akka;

import akka.actor.ActorRef;
import akka.actor.Props;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Akka 示例 - 演示 Actor 和 Stream 的使用
 */
public class AkkaExample {
    
    private static final Logger LOG = LoggerFactory.getLogger(AkkaExample.class);
    
    /**
     * 运行 Actor 示例
     */
    public void runActorExample() {
        LOG.info("=== 运行 Akka Actor 示例 ===");
        
        // 创建 ActorSystem
        AkkaClientUtil client = new AkkaClientUtil("actor-example-system");
        
        try {
            // 创建 Actor
            Props props = Props.create(ExampleActor.class, ExampleActor::new);
            ActorRef actorRef = client.createActor(props, "example-actor");
            
            // 发送消息给 Actor
            LOG.info("发送消息给 Actor...");
            ExampleActor.Message message = new ExampleActor.Message("Hello, Akka!", actorRef);
            actorRef.tell(message, ActorRef.noSender());
            
            // 等待一段时间让消息处理完成
            Thread.sleep(1000);
            
            LOG.info("Actor 示例执行完成");
        } catch (Exception e) {
            LOG.error("Actor 示例执行失败", e);
        } finally {
            // 关闭系统
            client.shutdown();
        }
    }
    
    /**
     * 运行 Stream 示例
     */
    public void runStreamExample() {
        LOG.info("=== 运行 Akka Stream 示例 ===");
        
        ExampleStream stream = new ExampleStream();
        
        try {
            // 运行简单示例
            stream.runSimpleExample();
            
            // 等待完成
            Thread.sleep(2000);
            
            LOG.info("Stream 示例执行完成");
        } catch (Exception e) {
            LOG.error("Stream 示例执行失败", e);
        } finally {
            // 关闭资源
            stream.shutdown();
        }
    }
    
    /**
     * 运行完整示例（包含 Actor 和 Stream）
     */
    public void runFullExample() {
        LOG.info("=== 运行 Akka 完整示例 ===");
        
        // 1. 运行 Actor 示例
        runActorExample();
        
        // 2. 运行 Stream 示例
        runStreamExample();
        
        LOG.info("所有 Akka 示例执行完成");
    }
}
