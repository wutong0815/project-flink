package com.example.project.base.model;

/**
 * 订单聚合结果模型
 * 
 * 用于存储按用户分组的订单统计结果
 */
public class OrderAgg {
    public String userId;
    public Double totalAmount;
    
    public OrderAgg() {}
    
    public OrderAgg(String userId, Double totalAmount) {
        this.userId = userId;
        this.totalAmount = totalAmount;
    }
    
    @Override
    public String toString() {
        return "OrderAgg{" +
                "userId='" + userId + '\'' +
                ", totalAmount=" + totalAmount +
                '}';
    }
}
