/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hudi.utilities.sources.helpers;

import org.apache.hudi.common.config.TypedProperties;
import org.apache.hudi.common.table.checkpoint.Checkpoint;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.collection.Pair;
import org.apache.hudi.utilities.exception.HoodieReadFromSourceException;
import org.apache.hudi.utilities.config.KinesisSourceConfig;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.kinesis.KinesisClient;
import software.amazon.awssdk.services.kinesis.KinesisClientBuilder;
import software.amazon.awssdk.services.kinesis.model.ExpiredIteratorException;
import software.amazon.awssdk.services.kinesis.model.GetRecordsRequest;
import software.amazon.awssdk.services.kinesis.model.GetRecordsResponse;
import software.amazon.awssdk.services.kinesis.model.GetShardIteratorRequest;
import software.amazon.awssdk.services.kinesis.model.InvalidArgumentException;
import software.amazon.awssdk.services.kinesis.model.LimitExceededException;
import software.amazon.awssdk.services.kinesis.model.ListShardsRequest;
import software.amazon.awssdk.services.kinesis.model.ListShardsResponse;
import software.amazon.awssdk.services.kinesis.model.ProvisionedThroughputExceededException;
import software.amazon.awssdk.services.kinesis.model.ResourceNotFoundException;
import software.amazon.awssdk.services.kinesis.model.Record;
import software.amazon.awssdk.services.kinesis.model.Shard;
import software.amazon.awssdk.services.kinesis.model.ShardIteratorType;

import java.net.URI;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import static org.apache.hudi.common.util.ConfigUtils.checkRequiredConfigProperties;
import static org.apache.hudi.common.util.ConfigUtils.getBooleanWithAltKeys;
import static org.apache.hudi.common.util.ConfigUtils.getLongWithAltKeys;
import static org.apache.hudi.common.util.ConfigUtils.getStringWithAltKeys;

/**
 * Helper for reading from Kinesis Data Streams and managing checkpoints.
 * Checkpoint format: streamName,shardId:sequenceNumber,shardId:sequenceNumber,...
 */
@Slf4j
@Getter
public class KinesisOffsetGen {

  public static class CheckpointUtils {
    /** Separator between lastSeq and endSeq for closed shards. Seq numbers are numeric, so this is safe. */
    private static final String END_SEQ_SEPARATOR = "|";
    /**
     * Kinesis checkpoint pattern.
     * Format: streamName,shardId:lastSeq,shardId:lastSeq|endSeq,...
     * For closed shards we store lastSeq|endSeq so we can detect data loss when shard expires.
     */
    private static final Pattern PATTERN = Pattern.compile(".*,.*:.*");

    /**
     * Parse checkpoint string to shardId -> value map. Value is lastSeq or lastSeq|endSeq for closed shards.
     */
    public static Map<String, String> strToOffsets(String checkpointStr) {
      Map<String, String> offsetMap = new HashMap<>();
      String[] splits = checkpointStr.split(",");
      for (int i = 1; i < splits.length; i++) {
        String part = splits[i];
        int colonIdx = part.indexOf(':');
        if (colonIdx > 0 && colonIdx < part.length() - 1) {
          String shardId = part.substring(0, colonIdx);
          String value = part.substring(colonIdx + 1);
          offsetMap.put(shardId, value);
        }
      }
      return offsetMap;
    }

    /**
     * Extract lastSeq from checkpoint value (which may be "lastSeq" or "lastSeq|endSeq").
     */
    public static String getLastSeqFromValue(String value) {
      if (value == null || value.isEmpty()) {
        return value;
      }
      int sep = value.indexOf(END_SEQ_SEPARATOR);
      return sep >= 0 ? value.substring(0, sep) : value;
    }

    /**
     * Extract endSeq from checkpoint value if present. Returns null for open shards.
     */
    public static String getEndSeqFromValue(String value) {
      if (value == null || value.isEmpty()) {
        return null;
      }
      int sep = value.indexOf(END_SEQ_SEPARATOR);
      return sep >= 0 && sep < value.length() - 1 ? value.substring(sep + 1) : null;
    }

    /**
     * Parse a checkpoint value into (lastSeq, endSeq). Combines {@link #getLastSeqFromValue} and
     * {@link #getEndSeqFromValue} into a single call to avoid parsing the value string twice.
     * @return Pair where left=lastSeq (empty Option when absent), right=endSeq (empty Option for open shards)
     */
    public static Pair<Option<String>, Option<String>> parseCheckpointValue(String value) {
      return Pair.of(Option.ofNullable(getLastSeqFromValue(value)),
          Option.ofNullable(getEndSeqFromValue(value)));
    }

    /**
     * Build checkpoint value: "lastSeq" or "lastSeq|endSeq" when endSeq is present (closed shards).
     */
    public static String buildCheckpointValue(String lastSeq, String endSeq) {
      if (endSeq != null && !endSeq.isEmpty()) {
        return lastSeq + END_SEQ_SEPARATOR + endSeq;
      }
      return lastSeq;
    }

    /**
     * String representation of checkpoint.
     * Format: streamName,shardId:value,shardId:value,... where value is lastSeq or lastSeq|endSeq.
     */
    public static String offsetsToStr(String streamName, Map<String, String> shardToValue) {
      String parts = shardToValue.entrySet().stream()
          .sorted(Map.Entry.comparingByKey())
          .map(e -> e.getKey() + ":" + e.getValue())
          .collect(Collectors.joining(","));
      return streamName + "," + parts;
    }

    /**
     * Returns true when {@code lastCheckpointStr} is a well-formed Kinesis checkpoint for {@code streamName}.
     * Checks both format (streamName,shardId:seq,...) and that the embedded stream name matches.
     */
    public static boolean isValidStreamCheckpoint(Option<String> lastCheckpointStr, String streamName) {
      return lastCheckpointStr.isPresent()
          && PATTERN.matcher(lastCheckpointStr.get()).matches()
          && lastCheckpointStr.get().startsWith(streamName + ",");
    }
  }

  /** The upper bound of the times that GetRecords function returns empty result */
  private static final int MAX_EMPTY_RESPONSES_FROM_GET_RECORDS = 100;

  /**
   * Represents a shard to read from, with optional starting sequence number.
   * For closed shards, endingSequenceNumber is set so we can store it in the checkpoint
   * and later detect data loss when the shard expires.
   */
  @AllArgsConstructor
  @Getter
  public static class KinesisShardRange implements java.io.Serializable {
    private final String shardId;
    /** If empty, use TRIM_HORIZON or LATEST based on config. */
    private final Option<String> startingSequenceNumber;
    /** For closed shards: the shard's ending sequence number. Empty for open shards. */
    private final Option<String> endingSequenceNumber;

    public static KinesisShardRange of(String shardId, Option<String> seqNum) {
      return new KinesisShardRange(shardId, seqNum, Option.empty());
    }

    public static KinesisShardRange of(String shardId, Option<String> seqNum, Option<String> endSeq) {
      return new KinesisShardRange(shardId, seqNum, endSeq);
    }

    /**
     * Returns true if this range may have unread records, false if we can definitively determine it has none.
     * Uses conservative default (useLatestWhenNoCheckpoint=false).
     */
    public boolean hasUnreadRecords() {
      return hasUnreadRecords(false);
    }

    /**
     * Returns true if this range may have unread records, false if we can definitively determine it has none.
     * <ul>
     *   <li>Open shard: always true (may have new records)</li>
     *   <li>Closed shard, lastSeq >= endSeq: false (fully consumed)</li>
     *   <li>Closed shard, no checkpoint and useLatest: false (at LATEST tip, closed shard has no records)</li>
     *   <li>Otherwise: true (may have unread records or cannot definitively say)</li>
     * </ul>
     *
     * @param useLatestWhenNoCheckpoint when startingSequenceNumber is empty, true means we use LATEST
     *        (start at tip); for a closed shard there are no records to read
     */
    public boolean hasUnreadRecords(boolean useLatestWhenNoCheckpoint) {
      String lastSeq = startingSequenceNumber.orElse(null);
      String endSeq = endingSequenceNumber.orElse(null);

      // CASE 1: Open shard: may have records
      if (endSeq == null || endSeq.isEmpty()) {
        return true;
      }
      // CASE 2: Closed shard with no checkpoint
      if (lastSeq == null || lastSeq.isEmpty()) {
        return !useLatestWhenNoCheckpoint;
      }
      // CASE 3: Closed shard: lastSeq >= endSeq means fully consumed
      if (lastSeq.compareTo(endSeq) >= 0) {
        return false;
      }
      // CASE 4: lastSeq < endSeq: may have unread records
      return true;
    }
  }

  private final String streamName;
  private final String region;
  private final Option<String> endpointUrl;
  private final KinesisSourceConfig.KinesisStartingPositionStrategy startingPositionStrategy;
  private final TypedProperties props;

  public KinesisOffsetGen(TypedProperties props) {
    this.props = props;
    checkRequiredConfigProperties(props,
        Arrays.asList(KinesisSourceConfig.KINESIS_STREAM_NAME, KinesisSourceConfig.KINESIS_REGION));
    this.streamName = getStringWithAltKeys(props, KinesisSourceConfig.KINESIS_STREAM_NAME);
    this.region = getStringWithAltKeys(props, KinesisSourceConfig.KINESIS_REGION);
    this.endpointUrl = Option.ofNullable(getStringWithAltKeys(props, KinesisSourceConfig.KINESIS_ENDPOINT_URL, null));
    this.startingPositionStrategy = KinesisSourceConfig.KinesisStartingPositionStrategy.fromString(
        getStringWithAltKeys(props, KinesisSourceConfig.KINESIS_STARTING_POSITION, true));
  }

  /**
   * Builds a Kinesis client from explicit parameters. Used by both the instance method
   * {@link #createKinesisClient()} and by {@link org.apache.hudi.utilities.sources.JsonKinesisSource}
   * from serializable {@link KinesisReadConfig} in Spark closures.
   */
  public static KinesisClient createKinesisClient(String region, String endpointUrl,
      String accessKey, String secretKey) {
    KinesisClientBuilder builder = KinesisClient.builder().region(Region.of(region));
    if (endpointUrl != null && !endpointUrl.isEmpty()) {
      builder = builder.endpointOverride(URI.create(endpointUrl));
    }
    if (accessKey != null && !accessKey.isEmpty() && secretKey != null && !secretKey.isEmpty()) {
      builder = builder.credentialsProvider(
          StaticCredentialsProvider.create(AwsBasicCredentials.create(accessKey, secretKey)));
    }
    return builder.build();
  }

  public KinesisClient createKinesisClient() {
    String accessKey = getStringWithAltKeys(props, KinesisSourceConfig.KINESIS_ACCESS_KEY, null);
    String secretKey = getStringWithAltKeys(props, KinesisSourceConfig.KINESIS_SECRET_KEY, null);
    return createKinesisClient(region, endpointUrl.orElse(null), accessKey, secretKey);
  }

  /**
   * List all active shards for the stream.
   * Note: AWS API disallows streamName and nextToken in the same request.
   */
  public List<Shard> listShards(KinesisClient client) {
    List<Shard> allShards = new ArrayList<>();
    String nextToken = null;
    do {
      ListShardsRequest request = nextToken != null
          ? ListShardsRequest.builder().nextToken(nextToken).build()
          : ListShardsRequest.builder().streamName(streamName).build();
      ListShardsResponse response;
      try {
        response = client.listShards(request);
      } catch (ResourceNotFoundException e) {
        throw new HoodieReadFromSourceException("Kinesis stream " + streamName + " not found", e);
      } catch (ProvisionedThroughputExceededException e) {
        throw new HoodieReadFromSourceException("Kinesis throughput exceeded listing shards for " + streamName, e);
      } catch (LimitExceededException e) {
        throw new HoodieReadFromSourceException("Kinesis limit exceeded listing shards: " + e.getMessage(), e);
      }
      allShards.addAll(response.shards());
      nextToken = response.nextToken();
    } while (nextToken != null);

    // Include both open and closed shards. Closed shards (e.g., from resharding) may still contain
    // unread records within the retention period. GetRecords works on closed shards until all data
    // is consumed, at which point NextShardIterator returns null.
    long numOpenShards = allShards.stream()
        .filter(s -> s.sequenceNumberRange() != null && s.sequenceNumberRange().endingSequenceNumber() == null)
        .count();
    log.info("Found {} shards for stream {} ({} open, {} closed)",
        allShards.size(), streamName, numOpenShards, allShards.size() - numOpenShards);
    logShardSequenceRanges(allShards);
    return allShards;
  }

  /**
   * Logs each shard's start/end sequence number so they can be used when resetting the checkpoint.
   */
  private void logShardSequenceRanges(List<Shard> shards) {
    for (Shard shard : shards) {
      String startSeq = (shard.sequenceNumberRange() != null && shard.sequenceNumberRange().startingSequenceNumber() != null)
          ? shard.sequenceNumberRange().startingSequenceNumber() : "n/a";
      String endSeq = (shard.sequenceNumberRange() != null && shard.sequenceNumberRange().endingSequenceNumber() != null)
          ? shard.sequenceNumberRange().endingSequenceNumber() : null;
      if (endSeq != null) {
        log.info("Shard {}: startSeq={}, endSeq={} (for checkpoint reset: {}:{}|{})",
            shard.shardId(), startSeq, endSeq, shard.shardId(), startSeq, endSeq);
      } else {
        log.info("Shard {}: startSeq={}, endSeq=open (for checkpoint reset from start: {}:{})",
            shard.shardId(), startSeq, shard.shardId(), startSeq);
      }
    }
  }

  /**
   * Get shard ranges to read, based on checkpoint and limits.
   */
  public KinesisShardRange[] getNextShardRanges(Option<Checkpoint> lastCheckpoint, long sourceLimit) {
    long numEvents = calculateNumEvents(sourceLimit, props);

    try (KinesisClient client = createKinesisClient()) {
      // STEP 1: List all open and closed shards from the server.
      // Note: no expired shards.
      List<Shard> shards = listShards(client);
      if (shards.isEmpty()) {
        return new KinesisShardRange[0];
      }
      // STEP 2: parse last checkpoint if exists.
      Map<String, String> fromSequenceNumbers = new HashMap<>();
      Option<String> lastCheckpointStr = lastCheckpoint.isPresent()
          ? Option.of(lastCheckpoint.get().getCheckpointKey()) : Option.empty();
      if (CheckpointUtils.isValidStreamCheckpoint(lastCheckpointStr, streamName)) {
        Map<String, String> checkpointOffsets = CheckpointUtils.strToOffsets(lastCheckpointStr.get());
        if (!checkpointOffsets.isEmpty()) {
          // Check for expired shards (checkpoint references shards no longer in stream, e.g., past retention)
          Set<String> availableShardIds = shards.stream().map(Shard::shardId).collect(Collectors.toSet());
          List<String> expiredShardIds = checkpointOffsets.keySet().stream()
              .filter(id -> !availableShardIds.contains(id))
              .collect(Collectors.toList());
          // Handle expired shards that exist in the last checkpoint.
          // This is important to detect data loss.
          if (!expiredShardIds.isEmpty()) {
            for (String shardId : expiredShardIds) {
              Pair<Option<String>, Option<String>> seqs = CheckpointUtils.parseCheckpointValue(checkpointOffsets.get(shardId));
              Option<String> lastSeqOpt = seqs.getLeft();
              Option<String> endSeqOpt = seqs.getRight();
              // endSeq absent = was open shard; conservatively assume not fully consumed (CASE 3).
              // endSeq present: fully consumed iff lastSeq >= endSeq (CASE 1/2).
              boolean fullyConsumed = endSeqOpt.isPresent()
                  && lastSeqOpt.map(last -> last.compareTo(endSeqOpt.get()) >= 0).orElse(false);
              if (fullyConsumed) {
                log.info("Expired shard {} was fully consumed (lastSeq >= endSeq); pruning from checkpoint",
                    shardId);
              } else {
                boolean failOnDataLoss = getBooleanWithAltKeys(props, KinesisSourceConfig.ENABLE_FAIL_ON_DATA_LOSS);
                String errorMessage = "Checkpoint references expired shard " + shardId
                    + " with unread data (lastSeq < endSeq or no endSeq stored). Data loss MAY have occurred. "
                    + "Set " + KinesisSourceConfig.ENABLE_FAIL_ON_DATA_LOSS.key() + "=false to continue.";
                if (failOnDataLoss) {
                  throw new HoodieReadFromSourceException(errorMessage);
                }
                log.warn(errorMessage);
              }
            }
          }
          // Handle regular case.
          // Parse lastSeq for open and closed shards.
          // For closed shards, even if all their records have been consumed, they are still included.
          for (String shardId : availableShardIds) {
            if (checkpointOffsets.containsKey(shardId)) {
              Option<String> lastSeqOpt = Option.ofNullable(
                  CheckpointUtils.getLastSeqFromValue(checkpointOffsets.get(shardId)))
                  .flatMap(seq -> seq.isEmpty() ? Option.empty() : Option.of(seq));
              lastSeqOpt.ifPresent(seq -> fromSequenceNumbers.put(shardId, seq));
            }
          }
        }
      }
      // STEP 3: Create ranges.
      List<KinesisShardRange> ranges = new ArrayList<>();
      for (Shard shard : shards) {
        String shardId = shard.shardId();
        Option<String> startSeq = fromSequenceNumbers.containsKey(shardId)
            ? Option.of(fromSequenceNumbers.get(shardId))
            : Option.empty();
        Option<String> endSeq = shard.sequenceNumberRange() != null
            ? Option.ofNullable(shard.sequenceNumberRange().endingSequenceNumber())
            : Option.empty();
        ranges.add(KinesisShardRange.of(shardId, startSeq, endSeq));
      }
      log.info("About to read up to {} events from {} shards in stream {})",
          numEvents, ranges.size(), streamName);
      return ranges.toArray(new KinesisShardRange[0]);
    } catch (ResourceNotFoundException e) {
      throw new HoodieReadFromSourceException("Kinesis stream " + streamName + " not found", e);
    } catch (ProvisionedThroughputExceededException e) {
      throw new HoodieReadFromSourceException("Kinesis throughput exceeded for stream " + streamName, e);
    } catch (InvalidArgumentException e) {
      throw new HoodieReadFromSourceException("Invalid Kinesis request: " + e.getMessage(), e);
    } catch (LimitExceededException e) {
      throw new HoodieReadFromSourceException("Kinesis limit exceeded: " + e.getMessage(), e);
    }
  }

  public static long calculateNumEvents(long sourceLimit, TypedProperties props) {
    long maxEvents = getLongWithAltKeys(props, KinesisSourceConfig.MAX_EVENTS_FROM_KINESIS_SOURCE);
    return sourceLimit == Long.MAX_VALUE ? maxEvents : Math.min(sourceLimit, maxEvents);
  }

  /**
   * Result of reading from a shard: records and the last sequence number for checkpoint.
   */
  @AllArgsConstructor
  @Getter
  public static class ShardReadResult implements java.io.Serializable {
    private final List<Record> records;
    private final Option<String> lastSequenceNumber;
    /** True when nextShardIterator was null, meaning the shard has no more records to return. */
    private final boolean reachedEndOfShard;
  }

  /**
   * Read records from a single shard.
   * @param enableDeaggregation when true, de-aggregates KPL records into individual user records
   */
  public static ShardReadResult readShardRecords(KinesisClient client, String streamName,
      KinesisShardRange range, KinesisSourceConfig.KinesisStartingPositionStrategy defaultPosition,
      int maxRecordsPerRequest, long intervalMs, long maxTotalRecords,
      boolean enableDeaggregation) throws InterruptedException {
    String shardIterator;
    try {
      shardIterator = getShardIterator(client, streamName, range, defaultPosition);
    } catch (InvalidArgumentException e) {
      // GetShardIterator throws InvalidArgumentException (not ExpiredIteratorException) when the
      // requested sequence number is past the stream's retention window.
      throw new HoodieReadFromSourceException("Sequence number in checkpoint is expired or invalid for shard "
          + range.getShardId() + ". Reset the checkpoint to recover.", e);
    } catch (ResourceNotFoundException e) {
      throw new HoodieReadFromSourceException("Shard or stream not found: " + range.getShardId(), e);
    } catch (ProvisionedThroughputExceededException e) {
      throw new HoodieReadFromSourceException("Kinesis throughput exceeded reading shard " + range.getShardId(), e);
    }
    List<Record> allRecords = new ArrayList<>();
    String lastSequenceNumber = null;
    int requestCount = 0;
    int emptyRecordRequestCount = 0;

    while (allRecords.size() < maxTotalRecords && shardIterator != null) {
      GetRecordsResponse response;
      try {
        response = client.getRecords(
            GetRecordsRequest.builder()
                .shardIterator(shardIterator)
                .limit(Math.min(maxRecordsPerRequest, (int) (maxTotalRecords - allRecords.size())))
                .build());
      } catch (ExpiredIteratorException e) {
        log.warn("Shard iterator expired for {} during GetRecords, stopping read", range.getShardId());
        break;
      } catch (ProvisionedThroughputExceededException e) {
        throw new HoodieReadFromSourceException("Kinesis throughput exceeded reading shard " + range.getShardId(), e);
      }

      List<Record> records = response.records();
      // Update shardIterator before the empty check so its null-ness correctly reflects end-of-shard
      // even when the final response carries 0 records (closed shard fully exhausted).
      shardIterator = response.nextShardIterator();
      // Process records from this response first, regardless of millisBehindLatest.
      if (!records.isEmpty()) {
        // CASE 1: records returned.
        List<Record> toAdd = enableDeaggregation ? KinesisDeaggregator.deaggregate(records) : records;
        for (Record r : toAdd) {
          allRecords.add(r);
        }
        // Checkpoint uses the last Kinesis record's sequence number (from raw records, not deaggregated)
        lastSequenceNumber = records.get(records.size() - 1).sequenceNumber();
      } else {
        // We break the loop to avoid infinite waiting when GetRecords always returns empty response.
        if (emptyRecordRequestCount++ > MAX_EMPTY_RESPONSES_FROM_GET_RECORDS) {
          break;
        }
      }
      // CASE 2: Caught up to the tip of the shard — no more records to fetch.
      // Check after processing so we don't discard records already in this response.
      // Note: millisBehindLatest can be 0 in LocalStack even when the response contained records,
      // so we must process first and stop second.
      if (response.millisBehindLatest() == 0) {
        break;
      }

      requestCount++;
      // This is for rate limiting
      if (shardIterator != null && intervalMs > 0) {
        try {
          Thread.sleep(intervalMs);
        } catch (InterruptedException e) {
          // Restore the interrupt flag before rethrowing: Thread.sleep clears it when it throws,
          // so any caller checking isInterrupted() (rather than catching IE) would miss the signal.
          Thread.currentThread().interrupt();
          throw e;
        }
      }
    }

    // Get here when
    // 1. reach the max total record limit
    // 2. or reach the end of the closed shard
    // NOTE that: There is a risk that getRecords keeps giving empty response, which could make us wait forever.

    log.debug("Read {} records from shard {} in {} requests", allRecords.size(), range.getShardId(), requestCount);
    return new ShardReadResult(allRecords, Option.ofNullable(lastSequenceNumber), shardIterator == null);
  }

  private static String getShardIterator(KinesisClient client, String streamName,
      KinesisShardRange range, KinesisSourceConfig.KinesisStartingPositionStrategy defaultPosition) {
    GetShardIteratorRequest.Builder builder = GetShardIteratorRequest.builder()
        .streamName(streamName)
        .shardId(range.getShardId());

    if (range.getStartingSequenceNumber().isPresent()) {
      builder.shardIteratorType(ShardIteratorType.AFTER_SEQUENCE_NUMBER);
      builder.startingSequenceNumber(range.getStartingSequenceNumber().get());
    } else {
      // EARLIEST is normalized to TRIM_HORIZON in constructor
      builder.shardIteratorType(defaultPosition == KinesisSourceConfig.KinesisStartingPositionStrategy.EARLIEST
          ? ShardIteratorType.TRIM_HORIZON : ShardIteratorType.LATEST);
    }
    return client.getShardIterator(builder.build()).shardIterator();
  }
}
