package com.example.project.base.model;

/**
 * 用户事件数据模型
 * 
 * 用于基础工作流中的用户行为事件表示
 */
public class UserEvent {
    public long userId;
    public String eventType;
    public long timestamp;
    
    public UserEvent() {}
    
    public UserEvent(long userId, String eventType, long timestamp) {
        this.userId = userId;
        this.eventType = eventType;
        this.timestamp = timestamp;
    }
    
    @Override
    public String toString() {
        return "UserEvent{" +
                "userId=" + userId +
                ", eventType='" + eventType + '\'' +
                ", timestamp=" + timestamp +
                '}';
    }
}
