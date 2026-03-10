package com.example.project.base.model;

/**
 * 订单数据模型
 * 
 * 用于 Kafka 流处理工作流中的订单数据表示
 */
public class Order {
    public String orderId;
    public String userId;
    public Double amount;
    public String product;
    public long timestamp;
    
    public Order() {}
    
    public Order(String orderId, String userId, Double amount, String product, long timestamp) {
        this.orderId = orderId;
        this.userId = userId;
        this.amount = amount;
        this.product = product;
        this.timestamp = timestamp;
    }
    
    @Override
    public String toString() {
        return "Order{" +
                "orderId='" + orderId + '\'' +
                ", userId='" + userId + '\'' +
                ", amount=" + amount +
                ", product='" + product + '\'' +
                ", timestamp=" + timestamp +
                '}';
    }
}
