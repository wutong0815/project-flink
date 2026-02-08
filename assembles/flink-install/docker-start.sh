#!/bin/bash

# Flink容器启动脚本
# 根据配置启动JobManager和TaskManager

set -e

# 初始化环境
/app/docker-init.sh

echo "启动Flink组件..."

# 从配置文件中获取参数
FLINK_CONF_FILE="/opt/flink/conf/flink-conf.yaml"

# 读取配置文件确定启动模式
if [ -f "$FLINK_CONF_FILE" ]; then
    # 检查是否应该启动JobManager
    if [ -z "$START_JOBMANAGER" ]; then
        if grep -q "jobmanager.rpc.address" "$FLINK_CONF_FILE" && ! grep -q "jobmanager.rpc.address: null" "$FLINK_CONF_FILE"; then
            START_JOBMANAGER="true"
        elif grep -q "kubernetes.cluster-id" "$FLINK_CONF_FILE"; then
            START_JOBMANAGER="true"
        else
            START_JOBMANAGER="false"
        fi
    fi
    
    # 检查是否应该启动TaskManager
    if [ -z "$START_TASKMANAGER" ]; then
        if grep -q "taskmanager.numberOfTaskSlots" "$FLINK_CONF_FILE" && ! grep -q "taskmanager.numberOfTaskSlots: 0" "$FLINK_CONF_FILE"; then
            START_TASKMANAGER="true"
        elif grep -q "kubernetes.cluster-id" "$FLINK_CONF_FILE"; then
            START_TASKMANAGER="true"
        else
            START_TASKMANAGER="false"
        fi
    fi
else
    # 默认情况下同时启动JobManager和TaskManager（本地模式）
    START_JOBMANAGER=${START_JOBMANAGER:-"true"}
    START_TASKMANAGER=${START_TASKMANAGER:-"true"}
fi

# 启动JobManager（如果需要）
if [ "$START_JOBMANAGER" = "true" ] || [ "$1" = "jobmanager" ]; then
    echo "启动Flink JobManager..."
    exec /opt/flink/bin/jobmanager.sh start-foreground cluster &
    JOBMANAGER_PID=$!
fi

# 启动TaskManager（如果需要）
if [ "$START_TASKMANAGER" = "true" ] || [ "$1" = "taskmanager" ]; then
    echo "启动Flink TaskManager..."
    exec /opt/flink/bin/taskmanager.sh start-foreground &
    TASKMANAGER_PID=$!
fi

# 如果指定了具体组件，则只启动该组件
if [ $# -gt 0 ]; then
    case $1 in
        "jobmanager")
            echo "等待JobManager进程..."
            wait $JOBMANAGER_PID
            ;;
        "taskmanager")
            echo "等待TaskManager进程..."
            wait $TASKMANAGER_PID
            ;;
        *)
            echo "未知组件: $1"
            exit 1
            ;;
    esac
else
    # 等待所有后台进程
    if [ ! -z "$JOBMANAGER_PID" ] && [ "$START_JOBMANAGER" = "true" ]; then
        wait $JOBMANAGER_PID
    fi
    
    if [ ! -z "$TASKMANAGER_PID" ] && [ "$START_TASKMANAGER" = "true" ]; then
        wait $TASKMANAGER_PID
    fi
fi

echo "Flink组件已停止"