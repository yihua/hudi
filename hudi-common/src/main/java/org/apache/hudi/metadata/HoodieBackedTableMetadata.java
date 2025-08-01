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

package org.apache.hudi.metadata;

import org.apache.hudi.avro.HoodieAvroReaderContext;
import org.apache.hudi.avro.HoodieAvroUtils;
import org.apache.hudi.avro.model.HoodieMetadataRecord;
import org.apache.hudi.common.config.HoodieCommonConfig;
import org.apache.hudi.common.config.HoodieMetadataConfig;
import org.apache.hudi.common.config.TypedProperties;
import org.apache.hudi.common.data.HoodieData;
import org.apache.hudi.common.data.HoodieListData;
import org.apache.hudi.common.data.HoodieListPairData;
import org.apache.hudi.common.data.HoodiePairData;
import org.apache.hudi.common.engine.HoodieEngineContext;
import org.apache.hudi.common.engine.HoodieReaderContext;
import org.apache.hudi.common.function.SerializableBiFunction;
import org.apache.hudi.common.function.SerializableFunction;
import org.apache.hudi.common.function.SerializableFunctionUnchecked;
import org.apache.hudi.common.model.FileSlice;
import org.apache.hudi.common.model.HoodieAvroRecord;
import org.apache.hudi.common.model.HoodieKey;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.model.HoodieRecordGlobalLocation;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.log.InstantRange;
import org.apache.hudi.common.table.read.HoodieFileGroupReader;
import org.apache.hudi.common.table.timeline.HoodieInstant;
import org.apache.hudi.common.table.view.HoodieTableFileSystemView;
import org.apache.hudi.common.util.HoodieDataUtils;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.StringUtils;
import org.apache.hudi.common.util.ValidationUtils;
import org.apache.hudi.common.util.collection.ClosableIterator;
import org.apache.hudi.common.util.collection.ClosableSortedDedupingIterator;
import org.apache.hudi.common.util.collection.CloseableMappingIterator;
import org.apache.hudi.common.util.collection.EmptyIterator;
import org.apache.hudi.common.util.collection.ImmutablePair;
import org.apache.hudi.common.util.collection.Pair;
import org.apache.hudi.exception.HoodieIOException;
import org.apache.hudi.exception.TableNotFoundException;
import org.apache.hudi.expression.BindVisitor;
import org.apache.hudi.expression.Expression;
import org.apache.hudi.expression.Literal;
import org.apache.hudi.expression.Predicate;
import org.apache.hudi.expression.Predicates;
import org.apache.hudi.internal.schema.Types;
import org.apache.hudi.storage.HoodieStorage;
import org.apache.hudi.storage.StoragePath;
import org.apache.hudi.storage.StoragePathInfo;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.IndexedRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.apache.hudi.common.config.HoodieCommonConfig.DISK_MAP_BITCASK_COMPRESSION_ENABLED;
import static org.apache.hudi.common.config.HoodieCommonConfig.SPILLABLE_DISK_MAP_TYPE;
import static org.apache.hudi.common.config.HoodieMemoryConfig.MAX_MEMORY_FOR_MERGE;
import static org.apache.hudi.common.config.HoodieMemoryConfig.SPILLABLE_MAP_BASE_PATH;
import static org.apache.hudi.common.config.HoodieMetadataConfig.DEFAULT_METADATA_ENABLE_FULL_SCAN_LOG_FILES;
import static org.apache.hudi.common.util.ValidationUtils.checkState;
import static org.apache.hudi.metadata.HoodieMetadataPayload.KEY_FIELD_NAME;
import static org.apache.hudi.metadata.HoodieTableMetadataUtil.PARTITION_NAME_BLOOM_FILTERS;
import static org.apache.hudi.metadata.HoodieTableMetadataUtil.PARTITION_NAME_COLUMN_STATS;
import static org.apache.hudi.metadata.HoodieTableMetadataUtil.PARTITION_NAME_FILES;
import static org.apache.hudi.metadata.HoodieTableMetadataUtil.existingIndexVersionOrDefault;
import static org.apache.hudi.metadata.HoodieTableMetadataUtil.getFileSystemViewForMetadataTable;
import static org.apache.hudi.metadata.SecondaryIndexKeyUtils.unescapeSpecialChars;

/**
 * Table metadata provided by an internal DFS backed Hudi metadata table.
 */
public class HoodieBackedTableMetadata extends BaseTableMetadata {

  private static final Logger LOG = LoggerFactory.getLogger(HoodieBackedTableMetadata.class);

  private final String metadataBasePath;

  private HoodieTableMetaClient metadataMetaClient;
  private Set<String> validInstantTimestamps = null;
  private HoodieTableFileSystemView metadataFileSystemView;
  // should we reuse the open file handles, across calls
  private final boolean reuse;

  // Latest file slices in the metadata partitions
  private final Map<String, List<FileSlice>> partitionFileSliceMap = new ConcurrentHashMap<>();

  public HoodieBackedTableMetadata(HoodieEngineContext engineContext,
                                   HoodieStorage storage,
                                   HoodieMetadataConfig metadataConfig,
                                   String datasetBasePath) {
    this(engineContext, storage, metadataConfig, datasetBasePath, false);
  }

  public HoodieBackedTableMetadata(HoodieEngineContext engineContext,
                                   HoodieStorage storage,
                                   HoodieMetadataConfig metadataConfig,
                                   String datasetBasePath, boolean reuse) {
    super(engineContext, storage, metadataConfig, datasetBasePath);
    this.reuse = reuse;
    this.metadataBasePath = HoodieTableMetadata.getMetadataTableBasePath(dataBasePath.toString());

    initIfNeeded();
  }

  private void initIfNeeded() {
    if (!isMetadataTableInitialized) {
      if (!HoodieTableMetadata.isMetadataTable(metadataBasePath)) {
        LOG.info("Metadata table is disabled.");
      }
    } else if (this.metadataMetaClient == null) {
      try {
        this.metadataMetaClient = HoodieTableMetaClient.builder()
            .setStorage(storage)
            .setBasePath(metadataBasePath)
            .build();
      } catch (TableNotFoundException e) {
        LOG.warn("Metadata table was not found at path {}", metadataBasePath);
        this.isMetadataTableInitialized = false;
        this.metadataMetaClient = null;
        this.metadataFileSystemView = null;
        this.validInstantTimestamps = null;
      } catch (Exception e) {
        LOG.error("Failed to initialize metadata table at path {}", metadataBasePath, e);
        this.isMetadataTableInitialized = false;
        this.metadataMetaClient = null;
        this.metadataFileSystemView = null;
        this.validInstantTimestamps = null;
      }
    }
  }

  @Override
  protected Option<HoodieRecord<HoodieMetadataPayload>> getRecordByKey(String key, String partitionName) {
    List<HoodieRecord<HoodieMetadataPayload>> records = getRecordsByKeys(
        HoodieListData.eager(Collections.singletonList(key)), partitionName, Option.empty())
        .values().collectAsList();
    ValidationUtils.checkArgument(records.size() <= 1, "Found more than 1 record for record key " + key);
    return records.isEmpty() ? Option.empty() : Option.ofNullable(records.get(0));
  }

  @Override
  public List<String> getPartitionPathWithPathPrefixUsingFilterExpression(List<String> relativePathPrefixes,
                                                                          Types.RecordType partitionFields,
                                                                          Expression expression) throws IOException {
    Expression boundedExpr = expression.accept(new BindVisitor(partitionFields, caseSensitive));
    List<String> selectedPartitionPaths = getPartitionPathWithPathPrefixes(relativePathPrefixes);

    // Can only prune partitions if the number of partition levels matches partition fields
    // Here we'll check the first selected partition to see whether the numbers match.
    if (hiveStylePartitioningEnabled
        && getPathPartitionLevel(partitionFields, selectedPartitionPaths.get(0)) == partitionFields.fields().size()) {
      return selectedPartitionPaths.stream()
          .filter(p ->
              (boolean) boundedExpr.eval(extractPartitionValues(partitionFields, p, urlEncodePartitioningEnabled)))
          .collect(Collectors.toList());
    }

    return selectedPartitionPaths;
  }

  @Override
  public List<String> getPartitionPathWithPathPrefixes(List<String> relativePathPrefixes) throws IOException {
    // TODO: consider skipping this method for non-partitioned table and simplify the checks
    return getAllPartitionPaths().stream()
        .filter(p -> relativePathPrefixes.stream().anyMatch(relativePathPrefix ->
            // Partition paths stored in metadata table do not have the slash at the end.
            // If the relativePathPrefix is empty, return all partition paths;
            // else if the relative path prefix is the same as the path, this is an exact match;
            // else, we need to make sure the path is a subdirectory of relativePathPrefix, by
            // checking if the path starts with relativePathPrefix appended by a slash ("/").
            StringUtils.isNullOrEmpty(relativePathPrefix)
                || p.equals(relativePathPrefix) || p.startsWith(relativePathPrefix + "/")))
        .collect(Collectors.toList());
  }

  @Override
  public HoodieData<HoodieRecord<HoodieMetadataPayload>> getRecordsByKeyPrefixes(HoodieData<String> keyPrefixes,
                                                                                 String partitionName,
                                                                                 boolean shouldLoadInMemory,
                                                                                 Option<SerializableFunctionUnchecked<String, String>> keyEncodingFn) {
    ValidationUtils.checkState(keyPrefixes instanceof HoodieListData, "getRecordsByKeyPrefixes only support HoodieListData at the moment");
    // Apply key encoding if present
    List<String> sortedKeyPrefixes = keyEncodingFn.isPresent()
        ? new ArrayList<>(keyPrefixes.map(k -> keyEncodingFn.get().apply(k)).collectAsList())
        : new ArrayList<>(keyPrefixes.collectAsList());
    // Sort the prefixes so that keys are looked up in order
    // Sort must come after encoding.
    Collections.sort(sortedKeyPrefixes);

    // NOTE: Since we partition records to a particular file-group by full key, we will have
    //       to scan all file-groups for all key-prefixes as each of these might contain some
    //       records matching the key-prefix
    List<FileSlice> partitionFileSlices = partitionFileSliceMap.computeIfAbsent(partitionName,
        k -> HoodieTableMetadataUtil.getPartitionLatestMergedFileSlices(metadataMetaClient, getMetadataFileSystemView(), partitionName));
    checkState(!partitionFileSlices.isEmpty(), "Number of file slices for partition " + partitionName + " should be > 0");

    return (shouldLoadInMemory ? HoodieListData.lazy(partitionFileSlices) :
        getEngineContext().parallelize(partitionFileSlices))
        .flatMap(
            (SerializableFunction<FileSlice, Iterator<HoodieRecord<HoodieMetadataPayload>>>) fileSlice ->
                getByKeyPrefixes(fileSlice, sortedKeyPrefixes, partitionName));
  }

  private Iterator<HoodieRecord<HoodieMetadataPayload>> getByKeyPrefixes(FileSlice fileSlice,
                                                                         List<String> sortedEncodedKeyPrefixes,
                                                                         String partitionName) throws IOException {
    HoodieFileGroupReader<IndexedRecord> fileGroupReader = buildFileGroupReader(sortedEncodedKeyPrefixes, fileSlice, false);
    ClosableIterator<IndexedRecord> it = fileGroupReader.getClosableIterator();
    return new CloseableMappingIterator<>(
        it,
        metadataRecord -> {
          HoodieMetadataPayload payload = new HoodieMetadataPayload(Option.of((GenericRecord) metadataRecord));
          String rowKey = payload.key != null
              ? payload.key : ((GenericRecord) metadataRecord).get(KEY_FIELD_NAME).toString();
          HoodieKey key = new HoodieKey(rowKey, partitionName);
          return new HoodieAvroRecord<>(key, payload);
        });
  }

  private Predicate transformKeysToPredicate(List<String> keys) {
    List<Expression> right = keys.stream().map(Literal::from).collect(Collectors.toList());
    return Predicates.in(null, right);
  }

  private Predicate transformKeyPrefixesToPredicate(List<String> keyPrefixes) {
    List<Expression> right = keyPrefixes.stream().map(Literal::from).collect(Collectors.toList());
    return Predicates.startsWithAny(null, right);
  }

  private HoodiePairData<String, HoodieRecord<HoodieMetadataPayload>> doLookup(HoodieData<String> keys, String partitionName, List<FileSlice> fileSlices,
                                                                               boolean isSecondaryIndex, Option<SerializableFunctionUnchecked<String, String>> keyEncodingFn) {

    final int numFileSlices = fileSlices.size();
    if (numFileSlices == 1) {
      List<String> keysList = keyEncodingFn.isPresent() ? keys.map(k -> keyEncodingFn.get().apply(k)).collectAsList() : keys.collectAsList();
      TreeSet<String> distinctSortedKeys = new TreeSet<>(keysList);
      return lookupKeyRecordPairs(partitionName, new ArrayList<>(distinctSortedKeys), fileSlices.get(0), !isSecondaryIndex);
    }

    // For SI v2, there are 2 cases require different implementation:
    // SI write path concatenates secKey$recordKey, the secKey needs extracted for hashing;
    // SI read path gives secKey only, no need for secKey extraction.
    SerializableBiFunction<String, Integer, Integer> mappingFunction = HoodieTableMetadataUtil::mapRecordKeyToFileGroupIndex;
    keys = repartitioningIfNeeded(keys, partitionName, numFileSlices, mappingFunction);
    HoodiePairData<Integer, String> persistedInitialPairData = keys
        // Tag key with file group index and apply key encoding
        .mapToPair(recordKey -> new ImmutablePair<>(
            mappingFunction.apply(recordKey, numFileSlices),
            keyEncodingFn.isPresent() ? keyEncodingFn.get().apply(recordKey) : recordKey));
    persistedInitialPairData.persist("MEMORY_AND_DISK_SER");
    // Use the new processValuesOfTheSameShards API instead of explicit rangeBasedRepartitionForEachKey
    SerializableFunction<Iterator<String>, Iterator<Pair<String, HoodieRecord<HoodieMetadataPayload>>>> processFunction =
        sortedKeys -> {
          List<String> keysList = new ArrayList<>();
          // Decorate with sorted stream deduplication.
          try (ClosableSortedDedupingIterator<String> distinctSortedKeyIter = new ClosableSortedDedupingIterator<>(sortedKeys)) {
            if (!distinctSortedKeyIter.hasNext()) {
              return Collections.emptyIterator();
            }
            distinctSortedKeyIter.forEachRemaining(keysList::add);
          }
          FileSlice fileSlice = fileSlices.get(mappingFunction.apply(unescapeSpecialChars(keysList.get(0)), numFileSlices));
          return lookupKeyRecordPairsItr(partitionName, keysList, fileSlice, !isSecondaryIndex);
        };

    List<Integer> keySpace = IntStream.range(0, numFileSlices).boxed().collect(Collectors.toList());
    HoodiePairData<String, HoodieRecord<HoodieMetadataPayload>> result =
        getEngineContext().mapGroupsByKey(persistedInitialPairData, processFunction, keySpace, true)
            .mapToPair(p -> Pair.of(p.getLeft(), p.getRight()));

    return result.filter((String k, HoodieRecord<HoodieMetadataPayload> v) -> !v.getData().isDeleted());
  }

  private HoodieData<HoodieRecord<HoodieMetadataPayload>> doLookupIndexRecords(HoodieData<String> keys, String partitionName, List<FileSlice> fileSlices,
                                                                               boolean isSecondaryIndex, Option<SerializableFunctionUnchecked<String, String>> keyEncodingFn) {

    final int numFileSlices = fileSlices.size();
    if (numFileSlices == 1) {
      List<String> keysList = keyEncodingFn.isPresent() ? keys.map(k -> keyEncodingFn.get().apply(k)).collectAsList() : keys.collectAsList();
      TreeSet<String> distinctSortedKeys = new TreeSet<>(keysList);
      return lookupRecords(partitionName, new ArrayList<>(distinctSortedKeys), fileSlices.get(0), !isSecondaryIndex);
    }

    // For SI v2, there are 2 cases require different implementation:
    // SI write path concatenates secKey$recordKey, the secKey needs extracted for hashing;
    // SI read path gives secKey only, no need for secKey extraction.
    SerializableBiFunction<String, Integer, Integer> mappingFunction = HoodieTableMetadataUtil::mapRecordKeyToFileGroupIndex;
    keys = repartitioningIfNeeded(keys, partitionName, numFileSlices, mappingFunction);
    HoodiePairData<Integer, String> persistedInitialPairData = keys
        // Tag key with file group index
        .mapToPair(recordKey -> new ImmutablePair<>(
            mappingFunction.apply(recordKey, numFileSlices),
            keyEncodingFn.isPresent() ? keyEncodingFn.get().apply(recordKey) : recordKey));
    persistedInitialPairData.persist("MEMORY_AND_DISK_SER");

    // Use the new processValuesOfTheSameShards API instead of explicit rangeBasedRepartitionForEachKey
    SerializableFunction<Iterator<String>, Iterator<HoodieRecord<HoodieMetadataPayload>>> processFunction =
        sortedKeys -> {
          List<String> keysList = new ArrayList<>();
          // Decorate with sorted stream deduplication.
          try (ClosableSortedDedupingIterator<String> distinctSortedKeyIter = new ClosableSortedDedupingIterator<>(sortedKeys)) {
            if (!distinctSortedKeyIter.hasNext()) {
              return Collections.emptyIterator();
            }
            distinctSortedKeyIter.forEachRemaining(keysList::add);
          }
          FileSlice fileSlice = fileSlices.get(mappingFunction.apply(unescapeSpecialChars(keysList.get(0)), numFileSlices));
          return lookupRecordsItr(partitionName, keysList, fileSlice, !isSecondaryIndex);
        };
    List<Integer> keySpace = IntStream.range(0, numFileSlices).boxed().collect(Collectors.toList());
    HoodieData<HoodieRecord<HoodieMetadataPayload>> result =
        getEngineContext().mapGroupsByKey(persistedInitialPairData, processFunction, keySpace, true);
    
    return result.filter((HoodieRecord<HoodieMetadataPayload> v) -> !v.getData().isDeleted());
  }

  /**
   * Reads record keys from record-level index. Deleted records are filtered out.
   * <p>
   * If the Metadata Table is not enabled, an exception is thrown to distinguish this from the absence of the key.
   *
   * @param recordKeys List of mapping from keys to the record location.
   */
  @Override
  public HoodiePairData<String, HoodieRecordGlobalLocation> readRecordIndex(HoodieData<String> recordKeys) {
    // If record index is not initialized yet, we cannot return an empty result here unlike the code for reading from other
    // indexes. This is because results from this function are used for upserts and returning an empty result here would lead
    // to existing records being inserted again causing duplicates.
    // The caller is required to check for record index existence in MDT before calling this method.
    ValidationUtils.checkState(dataMetaClient.getTableConfig().isMetadataPartitionAvailable(MetadataPartitionType.RECORD_INDEX),
        "Record index is not initialized in MDT");

    // TODO [HUDI-9544]: Metric does not work for rdd based API due to lazy evaluation.
    return getRecordsByKeys(recordKeys, MetadataPartitionType.RECORD_INDEX.getPartitionPath(), Option.empty())
        .mapToPair((Pair<String, HoodieRecord<HoodieMetadataPayload>> p) -> Pair.of(p.getLeft(), p.getRight().getData().getRecordGlobalLocation()));
  }

  /**
   * Reads record keys from record-level index. Deleted records are filtered out.
   * <p>
   * If the Metadata Table is not enabled, an exception is thrown to distinguish this from the absence of the key.
   *
   * @param recordKeys List of locations of record for the record keys.
   */
  @Override
  public HoodieData<HoodieRecordGlobalLocation> readRecordIndexLocations(HoodieData<String> recordKeys) {
    // If record index is not initialized yet, we cannot return an empty result here unlike the code for reading from other
    // indexes. This is because results from this function are used for upserts and returning an empty result here would lead
    // to existing records being inserted again causing duplicates.
    // The caller is required to check for record index existence in MDT before calling this method.
    ValidationUtils.checkState(dataMetaClient.getTableConfig().isMetadataPartitionAvailable(MetadataPartitionType.RECORD_INDEX),
        "Record index is not initialized in MDT");
    return readIndexRecords(recordKeys, MetadataPartitionType.RECORD_INDEX.getPartitionPath(), Option.empty())
        .map(r -> r.getData().getRecordGlobalLocation());
  }

  /**
   * Get record-location using secondary-index and record-index. Deleted records are filtered out.
   * <p>
   * If the Metadata Table is not enabled, an exception is thrown to distinguish this from the absence of the key.
   *
   * @param secondaryKeys The list of secondary keys to read
   */
  @Override
  public HoodiePairData<String, HoodieRecordGlobalLocation> readSecondaryIndex(HoodieData<String> secondaryKeys, String partitionName) {
    HoodieIndexVersion indexVersion = existingIndexVersionOrDefault(partitionName, dataMetaClient);

    if (indexVersion.equals(HoodieIndexVersion.V1)) {
      return readSecondaryIndexV1(secondaryKeys, partitionName);
    } else if (indexVersion.equals(HoodieIndexVersion.V2)) {
      return readSecondaryIndexV2(secondaryKeys, partitionName);
    } else {
      throw new IllegalArgumentException("readSecondaryIndex does not support index with version " + indexVersion);
    }
  }

  /**
   * Get record-location using secondary-index and record-index. Deleted records are filtered out.
   * <p>
   * If the Metadata Table is not enabled, an exception is thrown to distinguish this from the absence of the key.
   *
   * @param secondaryKeys The list of secondary keys to read
   */
  @Override
  public HoodieData<HoodieRecordGlobalLocation> readSecondaryIndexLocations(HoodieData<String> secondaryKeys, String partitionName) {
    HoodieIndexVersion indexVersion = existingIndexVersionOrDefault(partitionName, dataMetaClient);

    if (indexVersion.equals(HoodieIndexVersion.V1)) {
      return readSecondaryIndexV1(secondaryKeys, partitionName).values();
    } else if (indexVersion.equals(HoodieIndexVersion.V2)) {
      return readRecordIndexLocations(getRecordKeysFromSecondaryKeysV2(secondaryKeys, partitionName));
    } else {
      throw new IllegalArgumentException("readSecondaryIndexResult does not support index with version " + indexVersion);
    }
  }

  @Override
  public HoodiePairData<String, HoodieRecord<HoodieMetadataPayload>> getRecordsByKeys(
      HoodieData<String> keys, String partitionName, Option<SerializableFunctionUnchecked<String, String>> keyEncodingFn) {
    List<FileSlice> fileSlices = partitionFileSliceMap.computeIfAbsent(partitionName,
        k -> HoodieTableMetadataUtil.getPartitionLatestMergedFileSlices(metadataMetaClient, getMetadataFileSystemView(), partitionName));
    checkState(!fileSlices.isEmpty(), "No file slices found for partition: " + partitionName);

    boolean isSecondaryIndex = MetadataPartitionType.SECONDARY_INDEX.matchesPartitionPath(partitionName);
    return doLookup(keys, partitionName, fileSlices, isSecondaryIndex, keyEncodingFn);
  }

  public HoodieData<String> getRecordKeysFromSecondaryKeysV2(HoodieData<String> secondaryKeys, String partitionName) {
    return readIndexRecords(secondaryKeys, partitionName, Option.of(SecondaryIndexKeyUtils::escapeSpecialChars)).map(
        hoodieRecord -> SecondaryIndexKeyUtils.getRecordKeyFromSecondaryIndexKey(hoodieRecord.getRecordKey()));
  }

  private HoodiePairData<String, HoodieRecordGlobalLocation> readSecondaryIndexV2(HoodieData<String> secondaryKeys, String partitionName) {
    return readRecordIndex(getRecordKeysFromSecondaryKeysV2(secondaryKeys, partitionName));
  }

  private HoodiePairData<String, HoodieRecordGlobalLocation> readSecondaryIndexV1(HoodieData<String> secondaryKeys, String partitionName) {
    // For secondary index v1 we keep the old implementation.
    ValidationUtils.checkState(secondaryKeys instanceof HoodieListData, "readSecondaryIndex only support HoodieListData at the moment");
    ValidationUtils.checkState(dataMetaClient.getTableConfig().isMetadataPartitionAvailable(MetadataPartitionType.RECORD_INDEX),
        "Record index is not initialized in MDT");
    ValidationUtils.checkState(
        dataMetaClient.getTableConfig().getMetadataPartitions().contains(partitionName),
        "Secondary index is not initialized in MDT for: " + partitionName);
    // Fetch secondary-index records
    Map<String, Set<String>> secondaryKeyRecords = HoodieDataUtils.dedupeAndCollectAsMap(
        getSecondaryIndexRecords(HoodieListData.eager(secondaryKeys.collectAsList()), partitionName));
    // Now collect the record-keys and fetch the RLI records
    List<String> recordKeys = new ArrayList<>();
    secondaryKeyRecords.values().forEach(recordKeys::addAll);
    return readRecordIndex(HoodieListData.eager(recordKeys));
  }

  protected HoodieData<HoodieRecord<HoodieMetadataPayload>> readIndexRecords(HoodieData<String> keys,
                                                                             String partitionName,
                                                                             Option<SerializableFunctionUnchecked<String, String>> keyEncodingFn) {
    List<FileSlice> fileSlices = partitionFileSliceMap.computeIfAbsent(partitionName,
        k -> HoodieTableMetadataUtil.getPartitionLatestMergedFileSlices(metadataMetaClient, getMetadataFileSystemView(), partitionName));
    checkState(!fileSlices.isEmpty(), "No file slices found for partition: " + partitionName);

    boolean isSecondaryIndex = MetadataPartitionType.SECONDARY_INDEX.matchesPartitionPath(partitionName);
    return doLookupIndexRecords(keys, partitionName, fileSlices, isSecondaryIndex, keyEncodingFn);
  }

  // When testing we noticed that the parallelism can be very low which hurts the performance. so we should start with a reasonable
  // level of parallelism in that case.
  private HoodieData<String> repartitioningIfNeeded(
      HoodieData<String> keys, String partitionName, int numFileSlices, SerializableBiFunction<String, Integer, Integer> mappingFunction) {
    if (keys instanceof HoodieListData) {
      int parallelism = (int) keys.map(k -> mappingFunction.apply(k, numFileSlices)).distinct().count();
      // In case of empty lookup set, we should avoid RDD with 0 partitions.
      parallelism = Math.max(parallelism, 1);
      LOG.info("getRecordFast repartition HoodieListData to JavaRDD: exit, partitionName {}, num partitions: {}",
          partitionName, parallelism);
      keys = getEngineContext().parallelize(keys.collectAsList(), parallelism);
    } else if (keys.getNumPartitions() < metadataConfig.getRepartitionMinPartitionsThreshold()) {
      LOG.info("getRecordFast repartition HoodieNonListData. partitionName {}, num partitions: {}", partitionName, metadataConfig.getRepartitionDefaultPartitions());
      keys = keys.repartition(metadataConfig.getRepartitionDefaultPartitions());
    }
    return keys;
  }

  private HoodieFileGroupReader<IndexedRecord> buildFileGroupReader(List<String> sortedKeys,
                                                                    FileSlice fileSlice,
                                                                    boolean isFullKey) {
    Option<HoodieInstant> latestMetadataInstant =
        metadataMetaClient.getActiveTimeline().filterCompletedInstants().lastInstant();
    String latestMetadataInstantTime =
        latestMetadataInstant.map(HoodieInstant::requestedTime).orElse(SOLO_COMMIT_TIMESTAMP);
    Schema schema = HoodieAvroUtils.addMetadataFields(HoodieMetadataRecord.getClassSchema());
    // Only those log files which have a corresponding completed instant on the dataset should be read
    // This is because the metadata table is updated before the dataset instants are committed.
    Set<String> validInstantTimestamps = getValidInstantTimestamps();
    InstantRange instantRange = InstantRange.builder()
        .rangeType(InstantRange.RangeType.EXACT_MATCH)
        .explicitInstants(validInstantTimestamps).build();
    
    HoodieReaderContext<IndexedRecord> readerContext = new HoodieAvroReaderContext(
        storageConf,
        metadataMetaClient.getTableConfig(),
        Option.of(instantRange),
        Option.of(
            isFullKey
                ? transformKeysToPredicate(sortedKeys)
                : transformKeyPrefixesToPredicate(sortedKeys)));
    return HoodieFileGroupReader.<IndexedRecord>newBuilder()
        .withReaderContext(readerContext)
        .withHoodieTableMetaClient(metadataMetaClient)
        .withLatestCommitTime(latestMetadataInstantTime)
        .withFileSlice(fileSlice)
        .withDataSchema(schema)
        .withRequestedSchema(schema)
        .withProps(buildFileGroupReaderProperties(metadataConfig))
        .withStart(0)
        .withLength(Long.MAX_VALUE)
        .withShouldUseRecordPosition(false)
        .build();
  }

  private HoodiePairData<String, HoodieRecord<HoodieMetadataPayload>> lookupKeyRecordPairs(String partitionName,
                                                                                           List<String> sortedKeys,
                                                                                           FileSlice fileSlice,
                                                                                           Boolean isFullKey) {
    Map<String, List<HoodieRecord<HoodieMetadataPayload>>> map = new HashMap<>();
    try (ClosableIterator<Pair<String, HoodieRecord<HoodieMetadataPayload>>> iterator =
             lookupKeyRecordPairsItr(partitionName, sortedKeys, fileSlice, isFullKey)) {
      iterator.forEachRemaining(entry -> map.put(entry.getKey(), Collections.singletonList(entry.getValue())));
    }
    return HoodieListPairData.eager(map);
  }

  private HoodieData<HoodieRecord<HoodieMetadataPayload>> lookupRecords(String partitionName,
                                                                        List<String> sortedKeys,
                                                                        FileSlice fileSlice,
                                                                        Boolean isFullKey) {
    List<HoodieRecord<HoodieMetadataPayload>> res = new ArrayList<>();
    try (ClosableIterator<HoodieRecord<HoodieMetadataPayload>> iterator = lookupRecordsItr(partitionName, sortedKeys, fileSlice, isFullKey)) {
      iterator.forEachRemaining(res::add);
    }
    return HoodieListData.lazy(res);
  }

  private ClosableIterator<Pair<String, HoodieRecord<HoodieMetadataPayload>>> lookupKeyRecordPairsItr(String partitionName,
                                                                                                      List<String> sortedKeys,
                                                                                                      FileSlice fileSlice,
                                                                                                      Boolean isFullKey) {
    return lookupRecords(sortedKeys, fileSlice, isFullKey, metadataRecord -> {
      HoodieMetadataPayload payload = new HoodieMetadataPayload(Option.of(metadataRecord));
      String rowKey = payload.key != null ? payload.key : metadataRecord.get(KEY_FIELD_NAME).toString();
      HoodieKey hoodieKey = new HoodieKey(rowKey, partitionName);
      return Pair.of(rowKey, new HoodieAvroRecord<>(hoodieKey, payload));
    });
  }

  private ClosableIterator<HoodieRecord<HoodieMetadataPayload>> lookupRecordsItr(String partitionName,
                                                                                 List<String> keys,
                                                                                 FileSlice fileSlice,
                                                                                 Boolean isFullKey) {
    return lookupRecords(keys, fileSlice, isFullKey,
        metadataRecord -> {
          HoodieMetadataPayload payload = new HoodieMetadataPayload(Option.of(metadataRecord));
          return new HoodieAvroRecord<>(new HoodieKey(payload.key, partitionName), payload);
        });
  }

  /**
   * Lookup records and produce a lazy iterator of mapped HoodieRecords.
   */
  private <T> ClosableIterator<T> lookupRecords(List<String> sortedKeys,
                                                FileSlice fileSlice,
                                                Boolean isFullKey,
                                                SerializableFunctionUnchecked<GenericRecord, T> transformer) {
    // If no keys to lookup, we must return early, otherwise, the hfile lookup will return all records.
    if (sortedKeys.isEmpty()) {
      return new EmptyIterator<>();
    }
    try {
      HoodieFileGroupReader<IndexedRecord> fileGroupReader = buildFileGroupReader(sortedKeys, fileSlice, isFullKey);
      ClosableIterator<IndexedRecord> rawIterator = fileGroupReader.getClosableIterator();

      return new CloseableMappingIterator<>(rawIterator, record -> {
        GenericRecord metadataRecord = (GenericRecord) record;
        return transformer.apply(metadataRecord);
      });
    } catch (IOException e) {
      throw new HoodieIOException("Error merging records from metadata table for " + sortedKeys.size() + " keys", e);
    }
  }

  private Set<String> getValidInstantTimestamps() {
    if (validInstantTimestamps == null) {
      validInstantTimestamps = HoodieTableMetadataUtil.getValidInstantTimestamps(dataMetaClient, metadataMetaClient);
    }
    return validInstantTimestamps;
  }

  // NOTE: We're allowing eager full-scan of the log-files only for "files" partition.
  //       Other partitions (like "column_stats", "bloom_filters") will have to be fetched
  //       t/h point-lookups
  private boolean isFullScanAllowedForPartition(String partitionName) {
    switch (partitionName) {
      case PARTITION_NAME_FILES:
        return DEFAULT_METADATA_ENABLE_FULL_SCAN_LOG_FILES;

      case PARTITION_NAME_COLUMN_STATS:
      case PARTITION_NAME_BLOOM_FILTERS:
      default:
        return false;
    }
  }

  @Override
  public void close() {
    partitionFileSliceMap.clear();
    if (this.metadataFileSystemView != null) {
      this.metadataFileSystemView.close();
      this.metadataFileSystemView = null;
    }
  }

  public boolean enabled() {
    return isMetadataTableInitialized;
  }

  public HoodieTableMetaClient getMetadataMetaClient() {
    return metadataMetaClient;
  }

  public HoodieTableFileSystemView getMetadataFileSystemView() {
    if (metadataFileSystemView == null) {
      metadataFileSystemView = getFileSystemViewForMetadataTable(metadataMetaClient);
    }
    return metadataFileSystemView;
  }

  public Map<String, String> stats() {
    Set<String> allMetadataPartitionPaths = Arrays.stream(MetadataPartitionType.getValidValues()).map(MetadataPartitionType::getPartitionPath).collect(Collectors.toSet());
    return metrics.map(m -> m.getStats(true, metadataMetaClient, this, allMetadataPartitionPaths)).orElseGet(HashMap::new);
  }

  @Override
  public Option<String> getSyncedInstantTime() {
    if (metadataMetaClient != null) {
      Option<HoodieInstant> latestInstant = metadataMetaClient.getActiveTimeline().getDeltaCommitTimeline().filterCompletedInstants().lastInstant();
      if (latestInstant.isPresent()) {
        return Option.of(latestInstant.get().requestedTime());
      }
    }
    return Option.empty();
  }

  @Override
  public Option<String> getLatestCompactionTime() {
    if (metadataMetaClient != null) {
      Option<HoodieInstant> latestCompaction = metadataMetaClient.getActiveTimeline().getCommitAndReplaceTimeline().filterCompletedInstants().lastInstant();
      if (latestCompaction.isPresent()) {
        return Option.of(latestCompaction.get().requestedTime());
      }
    }
    return Option.empty();
  }

  @Override
  public void reset() {
    initIfNeeded();
    dataMetaClient.reloadActiveTimeline();
    if (metadataMetaClient != null) {
      metadataMetaClient.reloadActiveTimeline();
      if (metadataFileSystemView != null) {
        metadataFileSystemView.close();
      }
      metadataFileSystemView = null;
    }
    validInstantTimestamps = null;
    partitionFileSliceMap.clear();
  }

  @Override
  public int getNumFileGroupsForPartition(MetadataPartitionType partition) {
    partitionFileSliceMap.computeIfAbsent(partition.getPartitionPath(),
        k -> HoodieTableMetadataUtil.getPartitionLatestMergedFileSlices(metadataMetaClient,
            getMetadataFileSystemView(), partition.getPartitionPath()));
    return partitionFileSliceMap.get(partition.getPartitionPath()).size();
  }

  @Override
  public Map<Pair<String, StoragePath>, List<StoragePathInfo>> listPartitions(List<Pair<String, StoragePath>> partitionPathList) throws IOException {
    Map<String, Pair<String, StoragePath>> absoluteToPairMap = partitionPathList.stream()
        .collect(Collectors.toMap(
            pair -> pair.getRight().toString(),
            Function.identity()
        ));
    return getAllFilesInPartitions(
        partitionPathList.stream().map(pair -> pair.getRight().toString()).collect(Collectors.toList()))
        .entrySet().stream().collect(Collectors.toMap(
            entry -> absoluteToPairMap.get(entry.getKey()),
            Map.Entry::getValue
        ));
  }

  @Override
  public HoodiePairData<String, Set<String>> getSecondaryIndexRecords(HoodieData<String> secondaryKeys, String partitionName) {
    HoodieIndexVersion indexVersion = existingIndexVersionOrDefault(partitionName, dataMetaClient);

    if (indexVersion.equals(HoodieIndexVersion.V1)) {
      return getSecondaryIndexRecordsV1(secondaryKeys, partitionName);
    } else if (indexVersion.equals(HoodieIndexVersion.V2)) {
      return getSecondaryIndexRecordsV2(secondaryKeys, partitionName);
    } else {
      throw new IllegalArgumentException("getSecondaryIndexRecords does not support index with version " + indexVersion);
    }
  }

  private HoodiePairData<String, Set<String>> getSecondaryIndexRecordsV1(HoodieData<String> keys, String partitionName) {
    if (keys.isEmpty()) {
      return HoodieListPairData.eager(Collections.emptyList());
    }

    Map<String, Set<String>> res = getRecordsByKeyPrefixes(keys, partitionName, false, Option.of(SecondaryIndexKeyUtils::escapeSpecialChars))
            .map(record -> {
              if (!record.getData().isDeleted()) {
                return SecondaryIndexKeyUtils.getSecondaryKeyRecordKeyPair(record.getRecordKey());
              }
              return null;
            })
            .filter(Objects::nonNull)
            .collectAsList()
            .stream()
            .collect(HashMap::new,
                    (map, pair) -> map.computeIfAbsent(pair.getKey(), k -> new HashSet<>()).add(pair.getValue()),
                    (map1, map2) -> map2.forEach((k, v) -> map1.computeIfAbsent(k, key -> new HashSet<>()).addAll(v)));


    return HoodieListPairData.eager(
            res.entrySet()
                    .stream()
                    .collect(Collectors.toMap(
                            Map.Entry::getKey,
                            entry -> Collections.singletonList(entry.getValue())
                    ))
    );
  }

  private HoodiePairData<String, Set<String>> getSecondaryIndexRecordsV2(HoodieData<String> secondaryKeys, String partitionName) {
    if (secondaryKeys.isEmpty()) {
      return HoodieListPairData.eager(Collections.emptyList());
    }
    return readIndexRecords(secondaryKeys, partitionName, Option.of(SecondaryIndexKeyUtils::escapeSpecialChars))
        .filter(hoodieRecord -> !hoodieRecord.getData().isDeleted())
        .mapToPair(hoodieRecord -> SecondaryIndexKeyUtils.getRecordKeySecondaryKeyPair(hoodieRecord.getRecordKey()))
        .groupByKey()
        .mapToPair(p -> {
          HashSet<String> secKeys = new HashSet<>();
          p.getValue().iterator().forEachRemaining(secKeys::add);
          return Pair.of(p.getKey(), secKeys);
        });
  }

  /**
   * Derive necessary properties for FG reader.
   */
  TypedProperties buildFileGroupReaderProperties(HoodieMetadataConfig metadataConfig) {
    HoodieCommonConfig commonConfig = HoodieCommonConfig.newBuilder()
        .fromProperties(metadataConfig.getProps()).build();
    TypedProperties props = new TypedProperties();
    props.setProperty(
        MAX_MEMORY_FOR_MERGE.key(),
        Long.toString(metadataConfig.getMaxReaderMemory()));
    props.setProperty(
        SPILLABLE_MAP_BASE_PATH.key(),
        metadataConfig.getSplliableMapDir());
    props.setProperty(
        SPILLABLE_DISK_MAP_TYPE.key(),
        commonConfig.getSpillableDiskMapType().name());
    props.setProperty(
        DISK_MAP_BITCASK_COMPRESSION_ENABLED.key(),
        Boolean.toString(commonConfig.isBitCaskDiskMapCompressionEnabled()));
    return props;
  }
}
