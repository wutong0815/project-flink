package com.example.project.core.akka;

import akka.actor.ActorRef;
import akka.actor.ActorSystem;
import akka.actor.Props;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Akka 工具类
 * 
 * 提供 ActorSystem 管理和 Actor 创建功能
 */
public class AkkaClientUtil implements AutoCloseable {
    
    private static final Logger LOG = LoggerFactory.getLogger(AkkaClientUtil.class);
    
    private final ActorSystem actorSystem;
    private final String systemName;
    
    /**
     * 构造函数，创建 ActorSystem
     * 
     * @param systemName ActorSystem 名称
     */
    public AkkaClientUtil(String systemName) {
        this.systemName = systemName;
        this.actorSystem = ActorSystem.create(systemName);
        LOG.info("ActorSystem '{}' 创建成功", systemName);
    }
    
    /**
     * 获取 ActorSystem
     * 
     * @return ActorSystem 实例
     */
    public ActorSystem getActorSystem() {
        return actorSystem;
    }
    
    /**
     * 创建 Actor
     * 
     * @param props Props 配置
     * @param name Actor 名称
     * @return ActorRef 引用
     */
    public ActorRef createActor(Props props, String name) {
        ActorRef actorRef = actorSystem.actorOf(props, name);
        LOG.info("Actor '{}' 创建成功", name);
        return actorRef;
    }
    
    /**
     * 停止 Actor
     * 
     * @param actorRef Actor 引用
     */
    public void stopActor(ActorRef actorRef) {
        if (actorRef != null) {
            actorSystem.stop(actorRef);
            LOG.info("Actor 已停止");
        }
    }
    
    /**
     * 关闭 ActorSystem
     */
    public void shutdown() {
        LOG.info("正在关闭 ActorSystem '{}'...", systemName);
        actorSystem.terminate();
        
        try {
            // 等待终止
            actorSystem.getWhenTerminated().toCompletableFuture().join();
        } catch (Exception e) {
            LOG.error("关闭 ActorSystem 时出错", e);
        }
        
        LOG.info("ActorSystem '{}' 已关闭", systemName);
    }
    
    @Override
    public void close() {
        shutdown();
    }
    
    /**
     * 检查 ActorSystem 是否正在运行
     * 
     * @return true 如果正在运行
     */
    public boolean isRunning() {
        return !actorSystem.whenTerminated().isCompleted();
    }
}
