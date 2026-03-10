package com.example.project.core.akka;

import akka.actor.AbstractActor;
import akka.actor.ActorRef;
import akka.actor.ActorSystem;
import akka.actor.Props;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Akka Actor 示例
 * 
 * 演示了如何创建和使用 Akka Actor
 */
public class ExampleActor extends AbstractActor {
    
    private static final Logger LOG = LoggerFactory.getLogger(ExampleActor.class);
    
    /**
     * 消息类 - 用于 Actor 间通信
     */
    public static class Message {
        private final String content;
        private final ActorRef sender;
        
        public Message(String content, ActorRef sender) {
            this.content = content;
            this.sender = sender;
        }
        
        public String getContent() {
            return content;
        }
        
        public ActorRef getSender() {
            return sender;
        }
    }
    
    /**
     * 响应消息类
     */
    public static class Response {
        private final String content;
        
        public Response(String content) {
            this.content = content;
        }
        
        public String getContent() {
            return content;
        }
    }
    
    @Override
    public Receive createReceive() {
        return receiveBuilder()
            .match(Message.class, this::handleMessage)
            .matchAny(this::handleUnknown)
            .build();
    }
    
    private void handleMessage(Message message) {
        LOG.info("收到消息：{}", message.getContent());
        
        // 处理消息并回复
        String processedContent = "已处理：" + message.getContent();
        LOG.info("处理消息：{}", processedContent);
        
        // 回复给发送者
        if (message.getSender() != null) {
            message.getSender().tell(new Response(processedContent), getSelf());
        }
    }
    
    private void handleUnknown(Object message) {
        LOG.warn("收到未知消息类型：{}", message.getClass().getSimpleName());
        unhandled(message);
    }
}
