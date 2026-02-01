FROM flink:1.17.1-scala_2.12-java11

LABEL maintainer="your-name"

# 创建应用目录
WORKDIR /app

# 复制初始化和启动脚本
COPY ../docker-init.sh /app/
COPY ../docker-start.sh /app/

# 复制配置文件模板
COPY ../flink-conf.yaml /app/conf/
COPY ../log4j-console.properties /app/conf/

# 设置脚本权限
RUN chmod +x /app/docker-init.sh \
    && chmod +x /app/docker-start.sh \
    && chown -R flink:flink /app

# 切换到flink用户
USER flink

# 暴露端口
EXPOSE 8081 6123 6124

# 使用启动脚本作为入口点
ENTRYPOINT ["/app/docker-start.sh"]