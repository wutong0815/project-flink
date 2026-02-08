#!/bin/bash

# Flink容器初始化脚本
# 用于初始化配置文件、权限等

set -e

echo "开始初始化Flink环境..."

# 创建必要的目录
mkdir -p /app/logs
mkdir -p /app/data
mkdir -p /app/plugins
mkdir -p /app/jars
mkdir -p /opt/flink/conf

# 设置目录权限
chmod 755 /app/logs
chmod 755 /app/data
chmod 755 /app/plugins
chmod 755 /app/jars

# 设置Flink特定权限
chown -R flink:flink /app

# 复制核心Hadoop配置文件
if [ -f "/app/config/core-site.xml" ]; then
    echo "复制Hadoop核心配置文件..."
    cp /app/config/core-site.xml /opt/flink/conf/core-site.xml
    chown flink:flink /opt/flink/conf/core-site.xml
fi

# 如果存在外部配置文件，则使用它覆盖默认配置
if [ -f "/app/config/flink.conf" ]; then
    echo "加载外部配置文件..."
    
    # 读取配置文件并更新flink-conf.yaml
    if [ -f "/app/config/flink.conf" ]; then
        echo "正在处理外部配置文件..." 
        
        while IFS='=' read -r key value; do
            # 跳过注释行和空行
            [[ -z "$key" || "$key" =~ ^[[:space:]]*# ]] && continue
            
            # 确保key和value不为空
            if [[ -n "$key" && -n "$value" ]]; then
                # 检查配置项是否已存在于flink-conf.yaml中
                if grep -q "^${key}:" /opt/flink/conf/flink-conf.yaml; then
                    # 如果存在，则替换其值
                    sed -i "s|^${key}:.*|${key}: ${value}|g" /opt/flink/conf/flink-conf.yaml
                else
                    # 如果不存在，则添加新配置项
                    echo "${key}: ${value}" >> /opt/flink/conf/flink-conf.yaml
                fi
            fi
        done < /app/config/flink.conf
    fi
fi

# 更新日志配置
if [ -f "/app/config/log4j-console.properties" ]; then
    cp /app/config/log4j-console.properties /opt/flink/conf/log4j-console.properties
fi

# 确保插件目录包含Hadoop JARs
if [ -d "/app/hadoop-jars" ]; then
    echo "复制Hadoop JARs到插件目录..."
    cp -r /app/hadoop-jars/* /opt/flink/lib/
fi

echo "初始化完成"