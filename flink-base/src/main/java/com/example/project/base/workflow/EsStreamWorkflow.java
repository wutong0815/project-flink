package com.example.project.base.workflow;

import com.example.project.base.model.Order;
import com.example.project.core.es.EsSinkFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;

/**
 * Elasticsearch 流处理工作流
 * 
 * 演示如何将 Flink 数据流实时写入 Elasticsearch：
 * 1. 从数据生成器读取订单数据
 * 2. 进行简单聚合处理
 * 3. 实时写入 Elasticsearch
 * 
 * 部署说明:
 * - ES 服务地址通过环境变量 ES_HOSTS 传递（例如：http://es-pod:9200）
 * - 支持 Kubernetes Pod 部署，ES 作为独立服务运行
 */
public class EsStreamWorkflow {
    
    private static final Logger LOG = LoggerFactory.getLogger(EsStreamWorkflow.class);
    
    /**
     * 运行 ES 流处理工作流
     * 
     * @param esHosts ES 服务器地址（从环境变量或参数获取）
     */
  public void runEsWorkflow(String esHosts) {
        try {
            LOG.info("启动 Elasticsearch 流处理工作流...");
            LOG.info("Elasticsearch Hosts: {}", esHosts);
            
            // 创建执行环境
            StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
            env.setParallelism(2);
            
            // 模拟数据源（实际场景可从 Kafka 读取）
            DataStream<Order> orderStream = env.fromSequence(1, 1000)
                    .map(seq -> {
                        Order order = new Order();
                        order.setId("order-" + seq);
                        order.setAmount((double) (Math.random() * 1000));
                        order.setUserId("user-" + (seq % 100));
                        order.setTimestamp(System.currentTimeMillis());
                        order.setStatus(seq % 2 == 0 ? "COMPLETED" : "PENDING");
                        return order;
                    });
            
            // 写入 Elasticsearch
            orderStream.addSink(new EsSinkFunction<>(
                esHosts,                              // ES 地址
                "orders",                             // 索引名称
                500,                                  // 批量大小
                3000L,                                // 刷新间隔 (ms)
                order -> {                            // 文档映射函数
                    Map<String, Object> doc = new HashMap<>();
                    doc.put("orderId", order.getId());
                    doc.put("userId", order.getUserId());
                    doc.put("amount", order.getAmount());
                    doc.put("status", order.getStatus());
                    doc.put("timestamp", order.getTimestamp());
                    
                    // 使用 orderId 作为 ES 文档 ID（支持更新）
                    return new DocumentWithId(order.getId(), doc);
                }
            ));
            
            // 执行作业
            LOG.info("开始执行 Flink 作业...");
            env.execute("Elasticsearch Stream Workflow");
            
        } catch (Exception e) {
            LOG.error("ES 流处理工作流执行失败", e);
            throw new RuntimeException("ES 流处理工作流执行失败", e);
        }
    }
    
    /**
     * 带 ID 的文档包装类
     */
  public static class DocumentWithId extends HashMap<String, Object> {
        private final String docId;
        
     public DocumentWithId(String id, Map<String, Object> data) {
            super(data);
            this.docId = id;
           put("id", id);
        }
        
     public String getDocId() {
            return docId;
        }
    }
}
