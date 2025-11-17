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

# Script to run metadata table performance benchmarks with various configurations

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/../../../../../../../" && pwd)"
BENCHMARK_CLASS="org.apache.hudi.utilities.perf.MetadataTableReadWriteBenchmark"
OUTPUT_DIR="${OUTPUT_DIR:-/tmp/hudi-benchmarks}"

# Create output directory
mkdir -p "$OUTPUT_DIR"

# Function to run benchmark with specific parameters
run_benchmark() {
    local name=$1
    local jvm_opts=$2
    local benchmark_params=$3
    local output_file="$OUTPUT_DIR/benchmark_${name}_$(date +%Y%m%d_%H%M%S).txt"

    echo "========================================="
    echo "Running benchmark: $name"
    echo "JVM Options: $jvm_opts"
    echo "Parameters: $benchmark_params"
    echo "Output: $output_file"
    echo "========================================="

    java -cp "$PROJECT_ROOT/hudi-utilities/target/*:$PROJECT_ROOT/hudi-utilities/target/lib/*" \
        $jvm_opts \
        org.openjdk.jmh.Main \
        "$BENCHMARK_CLASS" \
        -f 1 \
        -wi 2 \
        -i 5 \
        -t 1 \
        $benchmark_params \
        -rf text \
        -rff "$output_file"

    echo "Benchmark $name completed. Results saved to $output_file"
    echo ""
}

# Build the project first
echo "Building project..."
cd "$PROJECT_ROOT"
mvn clean package -DskipTests -pl hudi-utilities -am

# Test 1: Delete block memory efficiency
echo "=== Test 1: Delete Block Memory Efficiency ==="
run_benchmark "delete_block_small_heap" \
    "-Xmx2g -Xms2g -XX:+UseG1GC -XX:+PrintGCDetails -XX:+PrintGCTimeStamps" \
    "-p deleteBlockSize=SMALL,MEDIUM,LARGE,VERY_LARGE -p logFileCount=1 -p recordCount=10000"

# Test 2: Large heap to establish baseline
echo "=== Test 2: Delete Block Baseline (Large Heap) ==="
run_benchmark "delete_block_large_heap" \
    "-Xmx8g -Xms8g -XX:+UseG1GC" \
    "-p deleteBlockSize=VERY_LARGE -p logFileCount=1 -p recordCount=100000"

# Test 3: Log file count scaling
echo "=== Test 3: Log File Count Scaling ==="
run_benchmark "log_file_scaling" \
    "-Xmx4g -Xms4g -XX:+UseG1GC" \
    "-p deleteBlockSize=MEDIUM -p logFileCount=1,5,10,20 -p recordCount=10000"

# Test 4: Record count scaling
echo "=== Test 4: Record Count Scaling ==="
run_benchmark "record_count_scaling" \
    "-Xmx4g -Xms4g -XX:+UseG1GC" \
    "-p deleteBlockSize=MEDIUM -p logFileCount=5 -p recordCount=1000,10000,100000"

# Test 5: With and without record positions
echo "=== Test 5: Record Positions Impact ==="
run_benchmark "record_positions" \
    "-Xmx4g -Xms4g -XX:+UseG1GC" \
    "-p deleteBlockSize=LARGE -p logFileCount=5 -p recordCount=10000 -p useRecordPositions=true,false"

# Test 6: Different GC algorithms
echo "=== Test 6: GC Algorithm Comparison ==="
for gc in "G1GC" "ZGC" "Shenandoah"; do
    if [ "$gc" == "ZGC" ] && java -version 2>&1 | grep -q "version \"11"; then
        gc_opts="-XX:+UnlockExperimentalVMOptions -XX:+UseZGC"
    elif [ "$gc" == "Shenandoah" ]; then
        gc_opts="-XX:+UnlockExperimentalVMOptions -XX:+UseShenandoahGC"
    else
        gc_opts="-XX:+Use${gc}"
    fi

    run_benchmark "gc_${gc,,}" \
        "-Xmx4g -Xms4g $gc_opts" \
        "-p deleteBlockSize=LARGE -p logFileCount=10 -p recordCount=10000"
done

# Test 7: Specific methods
echo "=== Test 7: Individual Method Benchmarks ==="
run_benchmark "methods" \
    "-Xmx4g -Xms4g -XX:+UseG1GC" \
    "-p deleteBlockSize=LARGE -p logFileCount=5 -p recordCount=10000 \
     -bm avgt \
     -prof gc \
     -e '(?!benchmark(DeleteBlockDeserialization|MergeHandleCompaction|AppendHandle)).*'"

# Generate summary report
echo "========================================="
echo "Generating summary report..."
echo "========================================="

SUMMARY_FILE="$OUTPUT_DIR/benchmark_summary_$(date +%Y%m%d_%H%M%S).md"

cat > "$SUMMARY_FILE" << EOF
# Metadata Table Benchmark Summary

Generated: $(date)

## Test Configurations

1. **Delete Block Memory Efficiency**: Tests memory usage with different delete block sizes
2. **Delete Block Baseline**: Establishes baseline performance with large heap
3. **Log File Scaling**: Measures impact of increasing log file count
4. **Record Count Scaling**: Measures impact of increasing record count
5. **Record Positions**: Compares positional vs key-based merging
6. **GC Algorithms**: Compares different garbage collectors
7. **Individual Methods**: Profiles specific benchmark methods

## Results Summary

EOF

# Extract key metrics from output files
for file in "$OUTPUT_DIR"/benchmark_*.txt; do
    if [ -f "$file" ]; then
        echo "### $(basename "$file" .txt)" >> "$SUMMARY_FILE"
        echo '```' >> "$SUMMARY_FILE"
        grep -A 5 "Benchmark" "$file" | head -20 >> "$SUMMARY_FILE"
        echo '```' >> "$SUMMARY_FILE"
        echo "" >> "$SUMMARY_FILE"
    fi
done

echo "Summary report generated: $SUMMARY_FILE"

# Check for potential OOM scenarios
echo "========================================="
echo "Checking for OOM risks..."
echo "========================================="

grep -l "OutOfMemoryError\|GC overhead limit exceeded" "$OUTPUT_DIR"/*.txt 2>/dev/null && {
    echo "WARNING: OOM errors detected in benchmarks!"
    echo "Check the following files for details:"
    grep -l "OutOfMemoryError\|GC overhead limit exceeded" "$OUTPUT_DIR"/*.txt
} || {
    echo "No OOM errors detected in benchmark runs."
}

echo ""
echo "All benchmarks completed. Results saved in: $OUTPUT_DIR"