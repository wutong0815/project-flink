#!/bin/bash

# ============================================
# 业务数据处理工作流 - 快速启动脚本
# ============================================

set -e

# 颜色定义
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# 配置变量
DB_HOST="${DB_HOST:-localhost}"
DB_PORT="${DB_PORT:-3306}"
DB_NAME="${DB_NAME:-flink_db}"
DB_USER="${DB_USER:-root}"
DB_PASSWORD="${DB_PASSWORD:-password}"

KAFKA_SERVERS="${KAFKA_SERVERS:-localhost:9092}"
ES_HOSTS="${ES_HOSTS:-http://localhost:9200}"

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(dirname "$SCRIPT_DIR")"

echo -e "${BLUE}============================================${NC}"
echo -e "${BLUE}   业务数据处理工作流 - 快速启动${NC}"
echo -e "${BLUE}============================================${NC}"
echo ""

# 函数：检查命令是否存在
check_command() {
    if ! command -v "$1" &> /dev/null; then
        echo -e "${RED}错误：未找到命令 '$1'，请先安装${NC}"
        exit 1
    fi
}

# 函数：检查服务是否可访问
check_service() {
    local service_name=$1
    local host=$2
    local port=$3
    
    echo -n "检查 $service_name 服务 ($host:$port) ... "
    
    if timeout 5 bash -c "cat < /dev/null > /dev/tcp/$host/$port" 2>/dev/null; then
        echo -e "${GREEN}✓ 可用${NC}"
        return 0
    else
        echo -e "${RED}✗ 不可用${NC}"
        return 1
    fi
}

# 步骤 1: 检查依赖命令
echo -e "${YELLOW}步骤 1: 检查系统依赖...${NC}"
check_command java
check_command mvn

java_version=$(java -version 2>&1 | head -n 1)
echo "  Java 版本：$java_version"

mvn_version=$(mvn -version 2>&1 | head -n 1)
echo "  Maven 版本：$mvn_version"
echo ""

# 步骤 2: 检查服务状态
echo -e "${YELLOW}步骤 2: 检查依赖服务...${NC}"

# 检查 MySQL
if ! check_service "MySQL" "$DB_HOST" "$DB_PORT"; then
    echo -e "${RED}提示：请确保 MySQL 服务已启动${NC}"
    echo "  启动命令：docker run -d --name mysql -e MYSQL_ROOT_PASSWORD=$DB_PASSWORD -p $DB_PORT:3306 mysql:8.0"
    exit 1
fi

# 检查 Kafka
KAFKA_HOST=$(echo $KAFKA_SERVERS | cut -d':' -f1)
KAFKA_PORT=$(echo $KAFKA_SERVERS | cut -d':' -f2)
if ! check_service "Kafka" "$KAFKA_HOST" "$KAFKA_PORT"; then
    echo -e "${RED}提示：请确保 Kafka 服务已启动${NC}"
    echo "  启动命令：docker run -d --name kafka -p 9092:9092 confluentinc/cp-kafka:latest"
    exit 1
fi

# 检查 Elasticsearch
ES_HOST=$(echo $ES_HOSTS | sed 's|http://||' | cut -d':' -f1)
ES_PORT=$(echo $ES_HOSTS | sed 's|http://||' | cut -d':' -f2)
if ! check_service "Elasticsearch" "$ES_HOST" "$ES_PORT"; then
    echo -e "${RED}提示：请确保 Elasticsearch 服务已启动${NC}"
    echo "  启动命令：docker run -d --name elasticsearch -p 9200:9200 -p 9300:9300 elasticsearch:8.11.0"
    exit 1
fi

echo ""

# 步骤 3: 初始化数据库
echo -e "${YELLOW}步骤 3: 初始化数据库...${NC}"

if [ -f "$SCRIPT_DIR/init-database.sql" ]; then
    echo "执行数据库初始化脚本..."
    
    if command -v mysql &> /dev/null; then
        mysql -h"$DB_HOST" -P"$DB_PORT" -u"$DB_USER" -p"$DB_PASSWORD" < "$SCRIPT_DIR/init-database.sql"
        echo -e "${GREEN}✓ 数据库初始化成功${NC}"
    else
        echo -e "${YELLOW}警告：未找到 mysql 客户端，跳过自动初始化${NC}"
        echo "请手动执行：mysql -h$DB_HOST -P$DB_PORT -u$DB_USER -p$DB_PASSWORD < $SCRIPT_DIR/init-database.sql"
    fi
else
    echo -e "${YELLOW}警告：未找到 init-database.sql 文件${NC}"
fi

echo ""

# 步骤 4: 创建 Kafka Topic
echo -e "${YELLOW}步骤 4: 创建 Kafka Topic...${NC}"

if command -v kafka-topics.sh &> /dev/null; then
    KAFKA_BIN=$(which kafka-topics.sh)
elif [ -f "/usr/local/kafka/bin/kafka-topics.sh" ]; then
    KAFKA_BIN="/usr/local/kafka/bin/kafka-topics.sh"
else
    KAFKA_BIN=""
fi

if [ -n "$KAFKA_BIN" ]; then
    echo "创建输入 Topic: business-orders"
    $KAFKA_BIN --bootstrap-server $KAFKA_SERVERS \
        --create --topic business-orders \
        --partitions 3 --replication-factor 1 --if-not-exists 2>/dev/null || true
    
    echo "创建输出 Topic: business-results"
    $KAFKA_BIN --bootstrap-server $KAFKA_SERVERS \
        --create --topic business-results \
        --partitions 3 --replication-factor 1 --if-not-exists 2>/dev/null || true
    
    echo -e "${GREEN}✓ Kafka Topic 创建成功${NC}"
else
    echo -e "${YELLOW}警告：未找到 kafka-topics.sh，跳过自动创建${NC}"
    echo "请手动创建 Topic:"
    echo "  kafka-topics.sh --bootstrap-server $KAFKA_SERVERS --create --topic business-orders --partitions 3 --replication-factor 1"
    echo "  kafka-topics.sh --bootstrap-server $KAFKA_SERVERS --create --topic business-results --partitions 3 --replication-factor 1"
fi

echo ""

# 步骤 5: 编译项目
echo -e "${YELLOW}步骤 5: 编译项目...${NC}"
cd "$PROJECT_ROOT"

echo "执行 Maven 编译..."
mvn clean package -DskipTests -q

if [ $? -eq 0 ]; then
    echo -e "${GREEN}✓ 编译成功${NC}"
else
    echo -e "${RED}✗ 编译失败${NC}"
    exit 1
fi

echo ""

# 步骤 6: 启动应用
echo -e "${YELLOW}步骤 6: 启动业务工作流...${NC}"

FLINKBASE_JAR="$PROJECT_ROOT/flink-base/target/flinkbase-0.0.1-SNAPSHOT.jar"

if [ ! -f "$FLINKBASE_JAR" ]; then
    echo -e "${RED}错误：未找到 JAR 文件 $FLINKBASE_JAR${NC}"
    exit 1
fi

echo "设置环境变量..."
export DB_URL="jdbc:mysql://$DB_HOST:$DB_PORT/$DB_NAME?useSSL=false&serverTimezone=UTC&characterEncoding=utf8"
export DB_USER="$DB_USER"
export DB_PASSWORD="$DB_PASSWORD"
export KAFKA_SERVERS="$KAFKA_SERVERS"
export ES_HOSTS="$ES_HOSTS"

echo ""
echo -e "${BLUE}============================================${NC}"
echo -e "${BLUE}   启动参数:${NC}"
echo -e "${BLUE}============================================${NC}"
echo "  DB_URL:      $DB_URL"
echo "  DB_USER:     $DB_USER"
echo "  KAFKA:       $KAFKA_SERVERS"
echo "  ES:          $ES_HOSTS"
echo "  JAR:         $FLINKBASE_JAR"
echo -e "${BLUE}============================================${NC}"
echo ""

echo -e "${GREEN}开始启动 Flink 业务工作流...${NC}"
echo -e "${YELLOW}按 Ctrl+C 停止作业${NC}"
echo ""

java -jar "$FLINKBASE_JAR" business
