package com.example.project.core.akka;

import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Akka 集成测试
 */
public class AkkaIntegrationTest {
    
    private static final Logger LOG = LoggerFactory.getLogger(AkkaIntegrationTest.class);
    
    @Test
    public void testAkkaClientUtil() {
        LOG.info("测试 AkkaClientUtil...");
        
        try (AkkaClientUtil client = new AkkaClientUtil("test-system")) {
            // 验证 ActorSystem 是否创建成功
            assert client.getActorSystem() != null;
            assert client.isRunning();
            
            LOG.info("AkkaClientUtil 测试通过");
        }
    }
    
    @Test
    public void testExampleActor() {
        LOG.info("测试 ExampleActor...");
        
        AkkaClientUtil client = new AkkaClientUtil("actor-test-system");
        
        try {
            // 创建 Actor
            var props = akka.actor.Props.create(ExampleActor.class, ExampleActor::new);
            var actorRef = client.createActor(props, "test-actor");
            
            assert actorRef != null;
            
            // 发送测试消息
            ExampleActor.Message message = new ExampleActor.Message("Test Message", actorRef);
            actorRef.tell(message, akka.actor.ActorRef.noSender());
            
            // 等待处理完成
            Thread.sleep(500);
            
            LOG.info("ExampleActor 测试通过");
        } catch (Exception e) {
            LOG.error("ExampleActor 测试失败", e);
            throw new RuntimeException(e);
        } finally {
            client.shutdown();
        }
    }
    
    @Test
    public void testExampleStream() {
        LOG.info("测试 ExampleStream...");
        
        ExampleStream stream = new ExampleStream();
        
        try {
            // 运行简单示例
            stream.runSimpleExample();
            
            // 等待完成
            Thread.sleep(1000);
            
            LOG.info("ExampleStream 测试通过");
        } catch (Exception e) {
            LOG.error("ExampleStream 测试失败", e);
            throw new RuntimeException(e);
        } finally {
            stream.shutdown();
        }
    }
}
