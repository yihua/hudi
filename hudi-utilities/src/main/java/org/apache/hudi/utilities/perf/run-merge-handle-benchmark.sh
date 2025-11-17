#!/bin/bash

# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

# Script to run FileGroupReaderBasedMergeHandle Benchmark

set -e

# Default values
SPARK_MASTER="local[4]"
EXECUTOR_MEMORY="4g"
DRIVER_MEMORY="4g"
NUM_ITERATIONS=10
WARMUP_ITERATIONS=2
MAX_MEMORY_COMPACTION=1024

# Function to print usage
usage() {
    echo "Usage: $0 [OPTIONS]"
    echo ""
    echo "Options:"
    echo "  --base-path PATH          Base path for MDT table (required)"
    echo "  --base-file FILE          Base file path (optional)"
    echo "  --log-files FILES         Comma-separated log files (optional)"
    echo "  --partition-path PATH     Partition path (default: '')"
    echo "  --file-id ID              File ID (default: 'test-file-id')"
    echo "  --num-iterations N        Number of iterations (default: 10)"
    echo "  --warmup-iterations N     Warmup iterations (default: 2)"
    echo "  --spark-master URL        Spark master URL (default: local[4])"
    echo "  --executor-memory MEM     Executor memory (default: 4g)"
    echo "  --driver-memory MEM       Driver memory (default: 4g)"
    echo "  --max-memory MB           Max memory for compaction in MB (default: 1024)"
    echo "  --s3                      Enable S3 mode (requires AWS credentials)"
    echo "  --aws-access-key KEY      AWS access key"
    echo "  --aws-secret-key KEY      AWS secret key"
    echo "  --aws-region REGION       AWS region (default: us-east-1)"
    echo "  --help                    Show this help message"
    echo ""
    echo "Examples:"
    echo "  # Local filesystem benchmark"
    echo "  $0 --base-path /tmp/hudi/mdt --base-file base.parquet --log-files log1.log,log2.log"
    echo ""
    echo "  # S3 benchmark"
    echo "  $0 --base-path s3a://bucket/path/mdt --s3 --aws-access-key KEY --aws-secret-key SECRET"
    exit 1
}

# Parse command line arguments
ARGS=""
S3_MODE=false

while [[ $# -gt 0 ]]; do
    case $1 in
        --base-path)
            BASE_PATH="$2"
            ARGS="$ARGS --base-path $2"
            shift 2
            ;;
        --base-file)
            ARGS="$ARGS --base-file $2"
            shift 2
            ;;
        --log-files)
            ARGS="$ARGS --log-files $2"
            shift 2
            ;;
        --partition-path)
            ARGS="$ARGS --partition-path $2"
            shift 2
            ;;
        --file-id)
            ARGS="$ARGS --file-id $2"
            shift 2
            ;;
        --num-iterations)
            NUM_ITERATIONS="$2"
            ARGS="$ARGS --num-iterations $2"
            shift 2
            ;;
        --warmup-iterations)
            WARMUP_ITERATIONS="$2"
            ARGS="$ARGS --warmup-iterations $2"
            shift 2
            ;;
        --spark-master)
            SPARK_MASTER="$2"
            shift 2
            ;;
        --executor-memory)
            EXECUTOR_MEMORY="$2"
            shift 2
            ;;
        --driver-memory)
            DRIVER_MEMORY="$2"
            shift 2
            ;;
        --max-memory)
            MAX_MEMORY_COMPACTION="$2"
            ARGS="$ARGS --max-memory-for-compaction $2"
            shift 2
            ;;
        --s3)
            S3_MODE=true
            shift
            ;;
        --aws-access-key)
            AWS_ACCESS_KEY="$2"
            ARGS="$ARGS --aws-access-key $2"
            shift 2
            ;;
        --aws-secret-key)
            AWS_SECRET_KEY="$2"
            ARGS="$ARGS --aws-secret-key $2"
            shift 2
            ;;
        --aws-region)
            AWS_REGION="$2"
            ARGS="$ARGS --aws-region $2"
            shift 2
            ;;
        --help)
            usage
            ;;
        *)
            echo "Unknown option: $1"
            usage
            ;;
    esac
done

# Validate required arguments
if [ -z "$BASE_PATH" ]; then
    echo "Error: --base-path is required"
    usage
fi

# Find the project root directory (where pom.xml is located)
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/../../../../../.." && pwd)"

echo "Project root: $PROJECT_ROOT"

# Check if the JAR exists, if not build it
JAR_PATH="$PROJECT_ROOT/hudi-utilities/target/hudi-utilities-*-SNAPSHOT.jar"

if ! ls $JAR_PATH 1> /dev/null 2>&1; then
    echo "JAR not found. Building the project..."
    cd "$PROJECT_ROOT"
    mvn clean package -DskipTests -pl hudi-utilities -am
fi

# Get the actual JAR file
JAR_FILE=$(ls $JAR_PATH | head -n 1)

if [ ! -f "$JAR_FILE" ]; then
    echo "Error: Could not find JAR file at $JAR_PATH"
    echo "Please build the project first with: mvn clean package -DskipTests"
    exit 1
fi

echo "Using JAR: $JAR_FILE"

# Build Spark submit command
SPARK_CMD="spark-submit \
    --class org.apache.hudi.utilities.perf.FileGroupReaderBasedMergeHandleBenchmark \
    --master $SPARK_MASTER \
    --executor-memory $EXECUTOR_MEMORY \
    --driver-memory $DRIVER_MEMORY \
    --conf spark.serializer=org.apache.spark.serializer.KryoSerializer \
    --conf spark.sql.shuffle.partitions=4"

# Add S3 configurations if in S3 mode
if [ "$S3_MODE" = true ]; then
    SPARK_CMD="$SPARK_CMD \
        --conf spark.hadoop.fs.s3a.impl=org.apache.hadoop.fs.s3a.S3AFileSystem \
        --conf spark.hadoop.fs.s3a.fast.upload=true \
        --conf spark.hadoop.fs.s3a.block.size=128M"

    if [ -n "$AWS_ACCESS_KEY" ] && [ -n "$AWS_SECRET_KEY" ]; then
        SPARK_CMD="$SPARK_CMD \
            --conf spark.hadoop.fs.s3a.access.key=$AWS_ACCESS_KEY \
            --conf spark.hadoop.fs.s3a.secret.key=$AWS_SECRET_KEY"
    fi
fi

# Add JAR and arguments
SPARK_CMD="$SPARK_CMD $JAR_FILE $ARGS"

echo "========================================="
echo "FileGroupReaderBasedMergeHandle Benchmark"
echo "========================================="
echo "Base Path: $BASE_PATH"
echo "Iterations: $NUM_ITERATIONS"
echo "Warmup Iterations: $WARMUP_ITERATIONS"
echo "Max Memory for Compaction: ${MAX_MEMORY_COMPACTION}MB"
echo "Spark Master: $SPARK_MASTER"
echo "Executor Memory: $EXECUTOR_MEMORY"
echo "Driver Memory: $DRIVER_MEMORY"
if [ "$S3_MODE" = true ]; then
    echo "Storage: S3"
else
    echo "Storage: Local/HDFS"
fi
echo "========================================="
echo ""
echo "Executing command:"
echo "$SPARK_CMD"
echo ""

# Run the benchmark
exec $SPARK_CMD