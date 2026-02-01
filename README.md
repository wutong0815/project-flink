# Flink 容器化部署方案

此项目提供了一套完整的Flink容器化解决方案，支持快速部署和配置Flink JobManager和TaskManager。

## 目录结构

```
.
├── Dockerfile           # Docker镜像构建文件
├── docker-init.sh       # 容器初始化脚本
├── docker-start.sh      # Flink组件启动脚本
├── flink-conf.yaml      # Flink配置文件模板
├── log4j-console.properties  # 日志配置文件
├── flink.conf           # Key-Value格式的配置文件
├── docker-compose.yml   # Docker Compose配置
└── README.md            # 本文档
```

## 功能特性

1. **灵活配置**：支持通过`.conf`文件进行Key-Value格式的配置
2. **权限管理**：自动设置适当的文件和目录权限
3. **组件分离**：可独立启动JobManager和TaskManager
4. **配置热加载**：支持外部配置文件动态覆盖默认配置

## 使用方法

### 1. 构建镜像

```bash
docker build -t flink-cluster .
```

### 2. 使用Docker Compose部署

```bash
# 创建配置目录
mkdir -p config
cp flink.conf config/

# 启动集群
docker-compose up -d
```

### 3. 手动运行单个组件

```bash
# 运行JobManager
docker run -d -p 8081:8081 -p 6123:6123 \
  -e START_JOBMANAGER=true \
  -e START_TASKMANAGER=false \
  flink-cluster

# 运行TaskManager
docker run -d \
  -e START_JOBMANAGER=false \
  -e START_TASKMANAGER=true \
  --link <jobmanager-container> \
  flink-cluster
```

### 4. 配置自定义参数

将你的配置文件放在`config/`目录下，支持以下文件：
- `flink.conf`：Key-Value格式的Flink配置
- `log4j-console.properties`：日志配置

## 配置说明

### flink.conf 示例

```conf
# JobManager配置
jobmanager.rpc.address=localhost
jobmanager.rpc.port=6123
jobmanager.memory.process.size=1600m

# TaskManager配置
taskmanager.numberOfTaskSlots=2
taskmanager.memory.process.size=2048m

# Web界面配置
webfrontend.port=8081
```

### 环境变量

- `START_JOBMANAGER`: 是否启动JobManager (true/false)
- `START_TASKMANAGER`: 是否启动TaskManager (true/false)

## 端口映射

- `8081`: Web UI端口
- `6123`: RPC端口
- `6124`: 数据传输端口

## 自定义扩展

你可以通过修改以下脚本来定制行为：

1. `docker-init.sh`：初始化脚本，处理配置文件和权限
2. `docker-start.sh`：启动脚本，控制组件启动逻辑

## 监控与日志

日志文件位于容器内的 `/app/logs` 目录下，可以通过挂载卷来持久化。