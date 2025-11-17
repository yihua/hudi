# FileGroupReaderBasedMergeHandle JMH Benchmark

This JMH (Java Microbenchmark Harness) benchmark measures the performance of merge operations similar to `FileGroupReaderBasedMergeHandle.doMerge()` without Spark dependencies.

## Overview

This benchmark focuses on the core merge logic:
1. Reading base files (Parquet)
2. Reading log files
3. Merging records based on keys
4. Writing merged output

**Note**: Since the actual `FileGroupReaderBasedMergeHandle.doMerge()` requires Spark dependencies, this benchmark simulates the core operations. For benchmarking with actual Spark integration, use the original `FileGroupReaderBasedMergeHandleBenchmark`.

## Building the Benchmark

### Prerequisites

- JDK 8 or higher
- Maven 3.6+
- JMH dependencies (automatically included via pom.xml)

### Build Steps

```bash
# Navigate to the project root
cd /path/to/hudi

# Build with JMH annotation processor
mvn clean package -DskipTests -pl hudi-utilities -am

# The benchmark will be compiled with JMH annotations processed
```

## Running the Benchmark

### Method 1: Using Java directly

```bash
# Run with default parameters
java -cp hudi-utilities/target/hudi-utilities-*.jar \
    org.apache.hudi.utilities.perf.FileGroupReaderBasedMergeHandleJMHBenchmark

# Run with custom JMH parameters
java -cp hudi-utilities/target/hudi-utilities-*.jar \
    org.apache.hudi.utilities.perf.FileGroupReaderBasedMergeHandleJMHBenchmark \
    -f 1 -wi 2 -i 10 -t 1
```

### Method 2: Using JMH Runner (if packaged as uber-jar)

```bash
java -jar target/benchmarks.jar \
    FileGroupReaderBasedMergeHandleJMHBenchmark \
    -f 1 -wi 5 -i 10
```

### Method 3: Maven JMH Plugin (if configured)

```bash
mvn clean test -pl hudi-utilities \
    -Dtest=FileGroupReaderBasedMergeHandleJMHBenchmark
```

## JMH Command Line Options

| Option | Description | Default |
|--------|-------------|---------|
| `-f` | Number of forks | 1 |
| `-wi` | Number of warmup iterations | 2 |
| `-i` | Number of measurement iterations | 10 |
| `-t` | Number of threads | 1 |
| `-to` | Timeout for each iteration | 10 min |
| `-r` | Time for each measurement iteration | 1 min |
| `-w` | Time for each warmup iteration | 1 min |

## Benchmark Parameters

Use `-p` option to set benchmark-specific parameters:

```bash
java -jar target/benchmarks.jar FileGroupReaderBasedMergeHandleJMHBenchmark \
    -p basePath=/tmp/hudi/mdt \
    -p baseFile=base_file.parquet \
    -p logFiles=log1.log,log2.log \
    -p partitionPath=files \
    -p fileId=test-file-001 \
    -p maxMemoryForCompaction=2048 \
    -p sortOutput=true
```

### Available Parameters

| Parameter | Description | Default |
|-----------|-------------|---------|
| `basePath` | Base path for MDT table (local or S3) | `/tmp/hudi/mdt` |
| `baseFile` | Path to base file (relative to base path) | `""` (empty) |
| `logFiles` | Comma-separated list of log file paths | `""` (empty) |
| `partitionPath` | Partition path within the table | `files` |
| `fileId` | File ID for the file group | `test-file-id` |
| `maxMemoryForCompaction` | Max memory for compaction in MB | `1024` |
| `useRecordPositions` | Use record positions for merging | `false` |
| `sortOutput` | Sort output records | `false` |

## Example Runs

### Local Storage Benchmark

```bash
# Basic benchmark with local files
java -cp hudi-utilities/target/hudi-utilities-*.jar \
    org.apache.hudi.utilities.perf.FileGroupReaderBasedMergeHandleJMHBenchmark \
    -p basePath=/tmp/hudi/mdt \
    -p baseFile=base_file.parquet \
    -p logFiles=log1.log,log2.log \
    -f 1 -wi 3 -i 10
```

### S3 Storage Benchmark

```bash
# Benchmark with S3 storage (requires AWS credentials)
export AWS_ACCESS_KEY_ID=your_key
export AWS_SECRET_ACCESS_KEY=your_secret

java -cp hudi-utilities/target/hudi-utilities-*.jar \
    org.apache.hudi.utilities.perf.FileGroupReaderBasedMergeHandleJMHBenchmark \
    -p basePath=s3a://my-bucket/path/to/mdt \
    -p baseFile=base_file.parquet \
    -p logFiles=log1.log,log2.log \
    -f 1 -wi 2 -i 5
```

### Multi-threaded Benchmark

```bash
# Run with multiple threads to test concurrent compaction
java -cp hudi-utilities/target/hudi-utilities-*.jar \
    org.apache.hudi.utilities.perf.FileGroupReaderBasedMergeHandleJMHBenchmark \
    -t 4 -f 1 -wi 2 -i 10
```

## Preparing Test Data

### Creating Test Files

To benchmark with actual files, you need to prepare base and log files:

1. **Base File**: A Parquet file containing the base dataset
2. **Log Files**: Hudi log files containing updates/deletes

You can create test files using:
- Existing Hudi tables (copy from `.hoodie/.metadata` directory)
- Hudi test utilities to generate sample data
- Custom data generation scripts

### Using Mock Data

If no files are provided, the benchmark will simulate I/O operations based on file sizes.

## Output Interpretation

JMH provides detailed statistics including:

```
Benchmark                                            Mode  Cnt   Score    Error  Units
FileGroupReaderBasedMergeHandleJMHBenchmark.doMerge avgt   10  234.567 ± 12.345  ms/op
```

- **Score**: Average time per operation
- **Error**: Standard deviation
- **Units**: Milliseconds per operation

### Performance Metrics

The benchmark measures:
- **doMerge**: Full merge operation including read, merge, and write
- **compactionSetup**: Time to initialize compaction components

## Performance Tuning

### Memory Settings

Adjust JVM heap size for large file compactions:
```bash
java -Xms8G -Xmx8G -jar target/benchmarks.jar ...
```

### JMH Profilers

Enable profilers for detailed analysis:
```bash
# GC profiler
java -jar target/benchmarks.jar ... -prof gc

# Stack profiler
java -jar target/benchmarks.jar ... -prof stack

# Perf profiler (Linux only)
java -jar target/benchmarks.jar ... -prof perf
```

## Troubleshooting

### Out of Memory Errors

- Increase heap size: `-Xmx8G`
- Reduce `maxMemoryForCompaction` parameter
- Use smaller test files

### File Not Found Errors

- Ensure base path exists
- Verify file paths are relative to base path
- Check file permissions

### S3 Access Issues

- Verify AWS credentials are set
- Check S3 bucket permissions
- Ensure correct region configuration

## Comparison with Spark-based Benchmark

| Aspect | JMH Benchmark | Spark Benchmark |
|--------|--------------|-----------------|
| Dependencies | Minimal (No Spark) | Full Spark |
| Execution | Single JVM | Distributed |
| Accuracy | Simulated merge | Actual doMerge() |
| Use Case | Quick performance testing | Production-like testing |
| Setup Complexity | Low | High |

## Advanced Usage

### Custom Workloads

Modify benchmark parameters to simulate different scenarios:

```bash
# Small files, high memory
-p maxMemoryForCompaction=4096 -p sortOutput=false

# Large files, limited memory
-p maxMemoryForCompaction=512 -p sortOutput=true

# Multiple log files
-p logFiles=log1.log,log2.log,log3.log,log4.log
```

### Continuous Benchmarking

For regression testing, integrate with CI/CD:

```bash
#!/bin/bash
# Run benchmark and save results
java -jar target/benchmarks.jar \
    FileGroupReaderBasedMergeHandleJMHBenchmark \
    -rf json -rff results.json

# Parse and compare with baseline
python analyze_benchmark.py results.json baseline.json
```

## Contributing

When modifying the benchmark:
1. Maintain compatibility with existing parameters
2. Document new parameters and options
3. Ensure benchmark remains deterministic
4. Test with various file sizes and configurations