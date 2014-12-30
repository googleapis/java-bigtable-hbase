package com.google.cloud.bigtable.hbase.adapters;

import com.google.api.client.util.Strings;
import com.google.bigtable.anviltop.AnviltopData;
import com.google.cloud.bigtable.hbase.BigtableConstants;
import com.google.common.base.Joiner;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;

import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HConstants;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;

/**
 * Adapt a single instance of an HBase {@link HColumnDescriptor} to
 * an instance of {@link AnviltopData.ColumnFamily}
 */
public class ColumnDescriptorAdapter {

  /**
   * Configuration keys that we can support unconditionally and we provide
   * a mapped version of the column descriptor to anviltop.
   */
  public static final Set<String> SUPPORTED_OPTION_KEYS =
      ImmutableSet.of(
          HColumnDescriptor.MIN_VERSIONS,
          HColumnDescriptor.TTL,
          HConstants.VERSIONS);

  /**
   * Configuration keys that we ignore unconditionally
   */
  public static final Set<String> IGNORED_OPTION_KEYS =
      ImmutableSet.of(
          HColumnDescriptor.COMPRESSION,
          HColumnDescriptor.COMPRESSION_COMPACT,
          HColumnDescriptor.DATA_BLOCK_ENCODING,
          HColumnDescriptor.BLOCKCACHE,
          HColumnDescriptor.CACHE_DATA_ON_WRITE,
          HColumnDescriptor.CACHE_INDEX_ON_WRITE,
          HColumnDescriptor.CACHE_BLOOMS_ON_WRITE,
          HColumnDescriptor.EVICT_BLOCKS_ON_CLOSE,
          HColumnDescriptor.CACHE_DATA_IN_L1,
          HColumnDescriptor.PREFETCH_BLOCKS_ON_OPEN,
          HColumnDescriptor.BLOCKSIZE,
          HColumnDescriptor.BLOOMFILTER,
          HColumnDescriptor.REPLICATION_SCOPE,
          HConstants.IN_MEMORY);

  /**
   * Configuration option values that we ignore as long as the value is the one specified below.
   * Any other value results in an exception.
   */
  public static final Map<String, String> SUPPORTED_OPTION_VALUES =
      ImmutableMap.of(
          HColumnDescriptor.KEEP_DELETED_CELLS, Boolean.toString(false),
          HColumnDescriptor.COMPRESS_TAGS, Boolean.toString(false));

  /**
   * Build a list of configuration keys that we don't know how to handle
   */
  public static List<String> getUnknownFeatures(HColumnDescriptor columnDescriptor) {
    List<String> unknownFeatures = new ArrayList<String>();
    for (Map.Entry<String, String> entry : columnDescriptor.getConfiguration().entrySet()) {
      String key = entry.getKey();
      if (!SUPPORTED_OPTION_KEYS.contains(key)
          && !IGNORED_OPTION_KEYS.contains(key)
          && !SUPPORTED_OPTION_VALUES.containsKey(key)) {
        unknownFeatures.add(key);
      }
    }
    return unknownFeatures;
  }

  /**
   * Build a Map of configuration keys and values describing configuration values we don't support.
   */
  public static Map<String, String> getUnsupportedFeatures(HColumnDescriptor columnDescriptor) {
    Map<String, String> unsupportedConfiguration = new HashMap<String, String>();

    Map<String, String> configuration = columnDescriptor.getConfiguration();
    for (Map.Entry<String, String> entry : SUPPORTED_OPTION_VALUES.entrySet()) {
      if (configuration.containsKey(entry.getKey())
          && configuration.get(entry.getKey()) != null
          && !entry.getValue().equals(configuration.get(entry.getKey()))) {
        unsupportedConfiguration.put(entry.getKey(), configuration.get(entry.getKey()));
      }
    }
    return unsupportedConfiguration;
  }

  /**
   * Throw an {@link UnsupportedOperationException} if the column descriptor cannot be adapted due
   * to it having unknown configuration keys.
   */
  public static void throwIfRequestingUnknownFeatures(HColumnDescriptor columnDescriptor) {
    List<String> unknownFeatures = getUnknownFeatures(columnDescriptor);
    if (!unknownFeatures.isEmpty()) {
      String featureString = String.format(
          "Unknown configuration options: [%s]",
          Joiner.on(", ").join(unknownFeatures));
      throw new UnsupportedOperationException(featureString);
    }
  }

  /**
   * Throw an {@link UnsupportedOperationException} if the column descriptor cannot be adapted due
   * to it having configuration values that are not supported.
   */
  public static void throwIfRequestingUnsupportedFeatures(HColumnDescriptor columnDescriptor) {
    Map<String, String> unsupportedConfiguration = getUnsupportedFeatures(columnDescriptor);
    if (!unsupportedConfiguration.isEmpty()) {
      List<String> configurationStrings = new ArrayList<String>(unsupportedConfiguration.size());
      for (Map.Entry<String, String> entry : unsupportedConfiguration.entrySet()) {
        configurationStrings.add(String.format("(%s: %s)", entry.getKey(), entry.getValue()));
      }

      String exceptionMessage = String.format(
          "Unsupported configuration options: %s",
          Joiner.on(",").join(configurationStrings));
      throw new UnsupportedOperationException(exceptionMessage);
    }
  }

  /**
   * Construct an anviltop GC expression from the given column descriptor.
   */
  public static String buildGarbageCollectionExpression(HColumnDescriptor columnDescriptor) {
    int maxVersions = columnDescriptor.getMaxVersions();
    int minVersions = columnDescriptor.getMinVersions();
    int ttlSeconds = columnDescriptor.getTimeToLive();
    long anviltopTtl = BigtableConstants.BIGTABLE_TIMEUNIT.convert(ttlSeconds, TimeUnit.SECONDS);

    Preconditions.checkState(minVersions < maxVersions,
        "HColumnDescriptor min versions must be less than max versions.");

    StringBuilder buffer = new StringBuilder();
    if (ttlSeconds != HColumnDescriptor.DEFAULT_TTL) {
      // minVersions only comes into play with a TTL:
      if (minVersions != HColumnDescriptor.DEFAULT_MIN_VERSIONS) {
        buffer.append(String.format("(age() > %s && versions() > %s)", anviltopTtl, minVersions));
      } else {
        buffer.append(String.format("(age() > %s)", anviltopTtl));
      }
    }
    // Since the HBase version default is 1, which may or may not be the same as Anviltop
    if (buffer.length() != 0) {
      buffer.append(" || ");
    }
    buffer.append(String.format("(versions() > %s)", maxVersions));

    return buffer.toString();
  }

  /**
   * Adapt a single instance of an HBase {@link HColumnDescriptor} to
   * an instance of {@link AnviltopData.ColumnFamily}
   */
  public AnviltopData.ColumnFamily.Builder adapt(HColumnDescriptor columnDescriptor) {
    throwIfRequestingUnknownFeatures(columnDescriptor);
    throwIfRequestingUnsupportedFeatures(columnDescriptor);

    AnviltopData.ColumnFamily.Builder resultBuilder = AnviltopData.ColumnFamily.newBuilder();

    resultBuilder.setName(columnDescriptor.getNameAsString());
    String gcExpression = buildGarbageCollectionExpression(columnDescriptor);
    if (!Strings.isNullOrEmpty(gcExpression)) {
      resultBuilder.setGcExpression(gcExpression);
    }

    return resultBuilder;
  }
}
