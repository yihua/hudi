# FileGroupReaderBasedMergeHandle Microbenchmark

This microbenchmark measures the performance of `FileGroupReaderBasedMergeHandle` for compacting MDT (Metadata Table) file groups.

## Features

- Supports both local and S3 cloud storage
- Configurable number of iterations and warmup runs
- Detailed performance statistics (min, max, avg, percentiles)
- Memory configuration options
- Support for base files and log files as input

## Building the Benchmark

First, compile the Hudi project:

```bash
mvn clean package -DskipTests -Dspark3.5 -Dscala-2.12
```

## Running the Benchmark

### Local Storage Example

```bash
spark-submit \
  --class org.apache.hudi.utilities.perf.FileGroupReaderBasedMergeHandleBenchmark \
  --master local[4] \
  --executor-memory 4g \
  --driver-memory 4g \
  hudi-utilities/target/hudi-utilities-*.jar \
  --base-path /tmp/hudi/mdt \
  --base-file base_file.parquet \
  --log-files log1.log,log2.log,log3.log \
  --partition-path "files" \
  --file-id "test-file-001" \
  --num-iterations 10 \
  --warmup-iterations 2 \
  --max-memory-for-compaction 2048
```

### S3 Storage Example

```bash
spark-submit \
  --class org.apache.hudi.utilities.perf.FileGroupReaderBasedMergeHandleBenchmark \
  --master local[4] \
  --executor-memory 4g \
  --driver-memory 4g \
  --conf spark.hadoop.fs.s3a.impl=org.apache.hadoop.fs.s3a.S3AFileSystem \
  hudi-utilities/target/hudi-utilities-*.jar \
  --base-path s3a://my-bucket/path/to/mdt \
  --base-file base_file.parquet \
  --log-files log1.log,log2.log \
  --aws-access-key YOUR_ACCESS_KEY \
  --aws-secret-key YOUR_SECRET_KEY \
  --aws-region us-east-1 \
  --partition-path "files" \
  --file-id "test-file-001" \
  --num-iterations 10 \
  --warmup-iterations 2
```

## Command Line Options

| Option | Description | Default |
|--------|-------------|---------|
| `--base-path` | Base path for MDT table (local or S3) | Required |
| `--base-file` | Path to base file (relative to base path) | Optional |
| `--log-files` | Comma-separated list of log file paths | Optional |
| `--partition-path` | Partition path within the table | "" |
| `--file-id` | File ID for the file group | "test-file-id" |
| `--num-iterations` | Number of benchmark iterations | 10 |
| `--warmup-iterations` | Number of warmup iterations | 2 |
| `--output-path` | Output path for compacted file | {base-path}/benchmark-output |
| `--spark-master` | Spark master URL | local[4] |
| `--spark-memory` | Spark executor memory | 4g |
| `--max-memory-for-compaction` | Max memory for compaction in MB | 1024 |
| `--use-record-positions` | Use record positions for merging | false |
| `--sort-output` | Sort output records | false |

### S3-specific Options

| Option | Description | Default |
|--------|-------------|---------|
| `--aws-access-key` | AWS access key for S3 | Optional |
| `--aws-secret-key` | AWS secret key for S3 | Optional |
| `--aws-session-token` | AWS session token for S3 | Optional |
| `--aws-region` | AWS region for S3 | us-east-1 |
| `--s3-endpoint` | Custom S3 endpoint | Optional |

## Preparing Test Data

### Creating Base and Log Files

You can create test MDT files using Hudi's test utilities or by running a Hudi write job with metadata table enabled.

Example to generate test data:

```java
// Use HoodieTestDataGenerator or actual Hudi write operations
// to create base and log files for testing
```

### Using Existing MDT Files

If you have an existing Hudi table with metadata:

1. Navigate to the `.hoodie/.metadata` directory in your Hudi table
2. Identify the file groups you want to benchmark
3. Use the base file and log files as input to the benchmark

## Output

The benchmark provides detailed statistics:

```
========================================
Benchmark Results
========================================
Number of iterations: 10
Base path: /tmp/hudi/mdt
Partition path: files
File ID: test-file-001
Base file: base_file.parquet
Log files: log1.log,log2.log
----------------------------------------
Min duration: 234 ms
Max duration: 567 ms
Avg duration: 345.67 ms
P50 (median): 342 ms
P75: 367 ms
P90: 445 ms
P95: 489 ms
P99: 556 ms
========================================
```

## Performance Tuning

### Memory Configuration

Adjust `--max-memory-for-compaction` based on your file sizes:
- Small files (<100MB): 512-1024 MB
- Medium files (100MB-1GB): 1024-2048 MB
- Large files (>1GB): 2048-4096 MB

### Spark Configuration

For better performance:
```bash
--conf spark.sql.adaptive.enabled=true \
--conf spark.sql.adaptive.coalescePartitions.enabled=true \
--conf spark.serializer=org.apache.spark.serializer.KryoSerializer
```

## Troubleshooting

### Out of Memory Errors

Increase memory settings:
- `--spark-memory`: Increase executor memory
- `--max-memory-for-compaction`: Increase compaction memory limit

### S3 Access Issues

Ensure:
- Correct AWS credentials are provided
- S3 bucket and path exist
- IAM permissions allow read/write access

### File Not Found Errors

Verify:
- Base path exists
- Base file and log files are relative to base path
- Files are accessible from Spark executors

## Advanced Usage

### Benchmarking Different Compaction Strategies

Modify the code to test different compaction configurations:
- Change small file size limits
- Test with different record merge strategies
- Vary the number and size of log files

### Comparing Storage Backends

Run the same benchmark with identical data on:
- Local filesystem
- HDFS
- S3
- Other cloud storage providers

Record and compare the results to understand storage impact on compaction performance.