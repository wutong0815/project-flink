FROM flink:1.17.1-scala_2.12-java11

LABEL maintainer="your-name"

# 安装必要的工具
RUN apt-get update && apt-get install -y curl wget && rm -rf /var/lib/apt/lists/*

# 设置工作目录
WORKDIR /app

# 下载并安装Hadoop AWS和S3A文件系统依赖
RUN mkdir -p /opt/flink/lib \
    && cd /tmp \
    && wget -q https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/3.3.4/hadoop-aws-3.3.4.jar \
    && wget -q https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-bundle/1.12.262/aws-java-sdk-bundle-1.12.262.jar \
    && wget -q https://repo1.maven.org/maven2/org/apache/commons/commons-lang3/3.12.0/commons-lang3-3.12.0.jar \
    && mv *.jar /opt/flink/lib/

# 复制应用程序JAR文件
COPY target/flink-savepoint-demo-1.0-SNAPSHOT.jar /app/flink-job.jar

# 复制初始化和启动脚本
COPY flink-install/docker-init.sh /app/
COPY flink-install/docker-start.sh /app/

# 复制配置文件
COPY core-site.xml /app/config/
COPY flink-install/flink/conf/flink-conf.yaml /app/config/flink-conf.yaml
COPY flink-install/flink/conf/log4j-console.properties /app/config/log4j-console.properties

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