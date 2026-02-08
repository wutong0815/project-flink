#!/bin/bash

# Flink Savepoint操作脚本
# 用于创建、管理和恢复savepoint

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
FLINK_HOME=${FLINK_HOME:-"/opt/flink"}
FLINK_CONF_DIR=${FLINK_CONF_DIR:-"${FLINK_HOME}/conf"}
FLINK_BIN_DIR="${FLINK_HOME}/bin"

# 默认配置
MINIO_ENDPOINT=${MINIO_ENDPOINT:-"http://minio:9000"}
MINIO_ACCESS_KEY=${MINIO_ACCESS_KEY:-"minioadmin"}
MINIO_SECRET_KEY=${MINIO_SECRET_KEY:-"minioadmin"}
SAVEPOINT_PATH=${SAVEPOINT_PATH:-"s3a://flink-savepoints/"}
CHECKPOINT_PATH=${CHECKPOINT_PATH:-"s3a://flink-checkpoints/"}

usage() {
    echo "用法: $0 [选项] [命令]"
    echo ""
    echo "选项:"
    echo "  -e, --endpoint ENDPOINT    MinIO端点 (默认: $MINIO_ENDPOINT)"
    echo "  -a, --access-key KEY       MinIO访问密钥 (默认: $MINIO_ACCESS_KEY)"
    echo "  -s, --secret-key KEY       MinIO秘密密钥 (默认: $MINIO_SECRET_KEY)"
    echo "  -p, --savepoint-path PATH  Savepoint路径 (默认: $SAVEPOINT_PATH)"
    echo "  -c, --checkpoint-path PATH Checkpoint路径 (默认: $CHECKPOINT_PATH)"
    echo ""
    echo "命令:"
    echo "  trigger <jobId>          为指定作业触发savepoint"
    echo "  stop <jobId>             停止指定作业并创建savepoint"
    echo "  list                     列出所有savepoint"
    echo "  restore <jarPath> <className> [args...]  从最新savepoint恢复作业"
    echo "  config                   显示当前配置"
    echo ""
    exit 1
}

# 解析命令行参数
while [[ $# -gt 0 ]]; do
    case $1 in
        -e|--endpoint)
            MINIO_ENDPOINT="$2"
            shift 2
            ;;
        -a|--access-key)
            MINIO_ACCESS_KEY="$2"
            shift 2
            ;;
        -s|--secret-key)
            MINIO_SECRET_KEY="$2"
            shift 2
            ;;
        -p|--savepoint-path)
            SAVEPOINT_PATH="$2"
            shift 2
            ;;
        -c|--checkpoint-path)
            CHECKPOINT_PATH="$2"
            shift 2
            ;;
        -h|--help)
            usage
            ;;
        -*)
            echo "未知选项: $1"
            usage
            ;;
        *)
            break
            ;;
    esac
done

# 检查必需的命令
if [ $# -lt 1 ]; then
    echo "错误: 缺少命令"
    usage
fi

COMMAND="$1"
shift

case "$COMMAND" in
    trigger)
        if [ $# -ne 1 ]; then
            echo "错误: trigger命令需要一个jobId参数"
            exit 1
        fi
        
        JOB_ID="$1"
        echo "为作业 $JOB_ID 触发savepoint到 $SAVEPOINT_PATH..."
        
        # 使用Flink CLI触发savepoint
        $FLINK_BIN_DIR/flink savepoint \
            --job-id $JOB_ID \
            --savepointPath $SAVEPOINT_PATH \
            --configDir $FLINK_CONF_DIR
        ;;
    
    stop)
        if [ $# -ne 1 ]; then
            echo "错误: stop命令需要一个jobId参数"
            exit 1
        fi
        
        JOB_ID="$1"
        echo "停止作业 $JOB_ID 并创建savepoint到 $SAVEPOINT_PATH..."
        
        # 使用Flink CLI停止作业并创建savepoint
        $FLINK_BIN_DIR/flink stop \
            --savepointPath $SAVEPOINT_PATH \
            --job-id $JOB_ID \
            $FLINK_CONF_DIR
        ;;
    
    list)
        echo "列出savepoint位置: $SAVEPOINT_PATH"
        # 这里可以添加实际列出savepoint的逻辑
        echo "注意: 实际的savepoint列表需要通过S3客户端或MinIO浏览器查看"
        ;;
    
    restore)
        if [ $# -lt 2 ]; then
            echo "错误: restore命令需要jar路径和类名参数"
            exit 1
        fi
        
        JAR_PATH="$1"
        CLASS_NAME="$2"
        shift 2
        PROGRAM_ARGS="$*"
        
        echo "从savepoint恢复作业: $JAR_PATH, 类: $CLASS_NAME"
        
        # 查找最新的savepoint并恢复
        # 注意: 实际实现中可能需要更复杂的逻辑来查找最新的savepoint
        $FLINK_BIN_DIR/flink run \
            -s $SAVEPOINT_PATH \
            -c $CLASS_NAME \
            $JAR_PATH $PROGRAM_ARGS
        ;;
    
    config)
        echo "当前配置:"
        echo "  MinIO Endpoint: $MINIO_ENDPOINT"
        echo "  MinIO Access Key: $MINIO_ACCESS_KEY"
        echo "  MinIO Secret Key: $MINIO_SECRET_KEY"
        echo "  Savepoint Path: $SAVEPOINT_PATH"
        echo "  Checkpoint Path: $CHECKPOINT_PATH"
        ;;
    
    *)
        echo "错误: 未知命令 '$COMMAND'"
        usage
        ;;
esac

echo "命令 '$COMMAND' 执行完成"