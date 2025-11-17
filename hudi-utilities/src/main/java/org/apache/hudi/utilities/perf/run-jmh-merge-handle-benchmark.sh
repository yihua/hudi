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

# Script to run FileGroupReaderBasedMergeHandle JMH Benchmark

set -e

# Default JMH values
FORKS=1
WARMUP_ITERATIONS=2
MEASUREMENT_ITERATIONS=10
THREADS=1
WARMUP_TIME="30s"
MEASUREMENT_TIME="60s"
JVM_HEAP="4g"

# Default benchmark parameters
BASE_PATH="/tmp/hudi/mdt"
BASE_FILE=""
LOG_FILES=""
PARTITION_PATH="files"
FILE_ID="test-file-id"
MAX_MEMORY_COMPACTION=1024
SORT_OUTPUT="false"
USE_RECORD_POSITIONS="false"

# JMH output format
OUTPUT_FORMAT="text"
OUTPUT_FILE=""

# Function to print usage
usage() {
    echo "Usage: $0 [OPTIONS]"
    echo ""
    echo "JMH Options:"
    echo "  --forks N                Number of benchmark forks (default: 1)"
    echo "  --warmup-iterations N    Warmup iterations (default: 2)"
    echo "  --iterations N           Measurement iterations (default: 10)"
    echo "  --threads N              Number of threads (default: 1)"
    echo "  --warmup-time TIME       Warmup time per iteration (default: 30s)"
    echo "  --measurement-time TIME  Measurement time per iteration (default: 60s)"
    echo "  --heap SIZE              JVM heap size (default: 4g)"
    echo ""
    echo "Benchmark Parameters:"
    echo "  --base-path PATH         Base path for MDT table (default: /tmp/hudi/mdt)"
    echo "  --base-file FILE         Base file path (optional)"
    echo "  --log-files FILES        Comma-separated log files (optional)"
    echo "  --partition-path PATH    Partition path (default: files)"
    echo "  --file-id ID             File ID (default: test-file-id)"
    echo "  --max-memory MB          Max memory for compaction in MB (default: 1024)"
    echo "  --sort-output            Enable output sorting (default: false)"
    echo "  --use-positions          Use record positions (default: false)"
    echo ""
    echo "Output Options:"
    echo "  --output-format FORMAT   Output format: text, csv, json (default: text)"
    echo "  --output-file FILE       Save results to file (optional)"
    echo ""
    echo "Other Options:"
    echo "  --profile PROFILER       Enable JMH profiler (gc, stack, perf, etc.)"
    echo "  --help                   Show this help message"
    echo ""
    echo "Examples:"
    echo "  # Basic benchmark"
    echo "  $0 --base-path /tmp/hudi/mdt --iterations 10"
    echo ""
    echo "  # Benchmark with files"
    echo "  $0 --base-path /tmp/hudi/mdt --base-file base.parquet --log-files log1.log,log2.log"
    echo ""
    echo "  # Multi-threaded benchmark with JSON output"
    echo "  $0 --threads 4 --output-format json --output-file results.json"
    echo ""
    echo "  # Benchmark with GC profiling"
    echo "  $0 --profile gc --iterations 5"
    exit 1
}

# Parse command line arguments
JMH_ARGS=""
PROFILER=""

while [[ $# -gt 0 ]]; do
    case $1 in
        --forks)
            FORKS="$2"
            shift 2
            ;;
        --warmup-iterations)
            WARMUP_ITERATIONS="$2"
            shift 2
            ;;
        --iterations)
            MEASUREMENT_ITERATIONS="$2"
            shift 2
            ;;
        --threads)
            THREADS="$2"
            shift 2
            ;;
        --warmup-time)
            WARMUP_TIME="$2"
            shift 2
            ;;
        --measurement-time)
            MEASUREMENT_TIME="$2"
            shift 2
            ;;
        --heap)
            JVM_HEAP="$2"
            shift 2
            ;;
        --base-path)
            BASE_PATH="$2"
            shift 2
            ;;
        --base-file)
            BASE_FILE="$2"
            shift 2
            ;;
        --log-files)
            LOG_FILES="$2"
            shift 2
            ;;
        --partition-path)
            PARTITION_PATH="$2"
            shift 2
            ;;
        --file-id)
            FILE_ID="$2"
            shift 2
            ;;
        --max-memory)
            MAX_MEMORY_COMPACTION="$2"
            shift 2
            ;;
        --sort-output)
            SORT_OUTPUT="true"
            shift
            ;;
        --use-positions)
            USE_RECORD_POSITIONS="true"
            shift
            ;;
        --output-format)
            OUTPUT_FORMAT="$2"
            shift 2
            ;;
        --output-file)
            OUTPUT_FILE="$2"
            shift 2
            ;;
        --profile)
            PROFILER="$2"
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

# Find the project root directory
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

# Build JMH arguments
JMH_ARGS="-f $FORKS"
JMH_ARGS="$JMH_ARGS -wi $WARMUP_ITERATIONS"
JMH_ARGS="$JMH_ARGS -i $MEASUREMENT_ITERATIONS"
JMH_ARGS="$JMH_ARGS -t $THREADS"

# Parse time values (JMH expects seconds as integer)
parse_time() {
    local time_str=$1
    if [[ $time_str == *s ]]; then
        echo "${time_str%s}"
    elif [[ $time_str == *m ]]; then
        echo $((${time_str%m} * 60))
    else
        echo "$time_str"
    fi
}

WARMUP_SECONDS=$(parse_time "$WARMUP_TIME")
MEASUREMENT_SECONDS=$(parse_time "$MEASUREMENT_TIME")

JMH_ARGS="$JMH_ARGS -w $WARMUP_SECONDS"
JMH_ARGS="$JMH_ARGS -r $MEASUREMENT_SECONDS"

# Add benchmark parameters
JMH_ARGS="$JMH_ARGS -p basePath=$BASE_PATH"
JMH_ARGS="$JMH_ARGS -p partitionPath=$PARTITION_PATH"
JMH_ARGS="$JMH_ARGS -p fileId=$FILE_ID"
JMH_ARGS="$JMH_ARGS -p maxMemoryForCompaction=$MAX_MEMORY_COMPACTION"
JMH_ARGS="$JMH_ARGS -p sortOutput=$SORT_OUTPUT"
JMH_ARGS="$JMH_ARGS -p useRecordPositions=$USE_RECORD_POSITIONS"

if [ -n "$BASE_FILE" ]; then
    JMH_ARGS="$JMH_ARGS -p baseFile=$BASE_FILE"
fi

if [ -n "$LOG_FILES" ]; then
    JMH_ARGS="$JMH_ARGS -p logFiles=$LOG_FILES"
fi

# Add output format
case $OUTPUT_FORMAT in
    csv)
        JMH_ARGS="$JMH_ARGS -rf csv"
        ;;
    json)
        JMH_ARGS="$JMH_ARGS -rf json"
        ;;
    *)
        JMH_ARGS="$JMH_ARGS -rf text"
        ;;
esac

# Add output file if specified
if [ -n "$OUTPUT_FILE" ]; then
    JMH_ARGS="$JMH_ARGS -rff $OUTPUT_FILE"
fi

# Add profiler if specified
if [ -n "$PROFILER" ]; then
    JMH_ARGS="$JMH_ARGS -prof $PROFILER"
fi

# Build Java command
JAVA_CMD="java -Xms$JVM_HEAP -Xmx$JVM_HEAP"

# Add S3 configurations if using S3 path
if [[ "$BASE_PATH" == s3://* ]] || [[ "$BASE_PATH" == s3a://* ]]; then
    echo "Detected S3 path. Ensuring AWS credentials are available..."
    if [ -z "$AWS_ACCESS_KEY_ID" ]; then
        echo "Warning: AWS_ACCESS_KEY_ID not set. S3 access may fail."
    fi
fi

# Add classpath and main class
JAVA_CMD="$JAVA_CMD -cp $JAR_FILE"
JAVA_CMD="$JAVA_CMD org.apache.hudi.utilities.perf.FileGroupReaderBasedMergeHandleJMHBenchmark"

echo "========================================"
echo "JMH Merge Handle Benchmark"
echo "========================================"
echo "Configuration:"
echo "  Forks: $FORKS"
echo "  Warmup: $WARMUP_ITERATIONS iterations x $WARMUP_TIME"
echo "  Measurement: $MEASUREMENT_ITERATIONS iterations x $MEASUREMENT_TIME"
echo "  Threads: $THREADS"
echo "  JVM Heap: $JVM_HEAP"
echo ""
echo "Benchmark Parameters:"
echo "  Base Path: $BASE_PATH"
echo "  Base File: ${BASE_FILE:-none}"
echo "  Log Files: ${LOG_FILES:-none}"
echo "  Partition Path: $PARTITION_PATH"
echo "  File ID: $FILE_ID"
echo "  Max Memory: ${MAX_MEMORY_COMPACTION}MB"
echo "  Sort Output: $SORT_OUTPUT"
echo ""
echo "Output:"
echo "  Format: $OUTPUT_FORMAT"
if [ -n "$OUTPUT_FILE" ]; then
    echo "  File: $OUTPUT_FILE"
fi
if [ -n "$PROFILER" ]; then
    echo "  Profiler: $PROFILER"
fi
echo "========================================"
echo ""
echo "Executing command:"
echo "$JAVA_CMD $JMH_ARGS"
echo ""

# Run the benchmark
exec $JAVA_CMD $JMH_ARGS