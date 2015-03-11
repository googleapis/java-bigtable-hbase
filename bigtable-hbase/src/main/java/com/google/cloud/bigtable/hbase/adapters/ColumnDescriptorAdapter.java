package com.google.cloud.bigtable.hbase.adapters;

import com.google.api.client.util.Strings;
import com.google.bigtable.admin.table.v1.ColumnFamily;
import com.google.cloud.bigtable.hbase.BigtableConstants;
import com.google.common.base.Joiner;
import com.google.common.base.Preconditions;
import com.google.common.base.Splitter;
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
 * an instance of {@link ColumnFamily}
 */
public class ColumnDescriptorAdapter {

  /**
   * Configuration keys that we can support unconditionally and we provide
   * a mapped version of the column descriptor to Bigtable.
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
   * Construct an Bigtable GC expression from the given column descriptor.
   */
  public static String buildGarbageCollectionExpression(HColumnDescriptor columnDescriptor) {
    int maxVersions = columnDescriptor.getMaxVersions();
    int minVersions = columnDescriptor.getMinVersions();
    int ttlSeconds = columnDescriptor.getTimeToLive();
    long bigtableTtl = BigtableConstants.BIGTABLE_TIMEUNIT.convert(ttlSeconds, TimeUnit.SECONDS);

    Preconditions.checkState(minVersions < maxVersions,
        "HColumnDescriptor min versions must be less than max versions.");

    StringBuilder buffer = new StringBuilder();
    if (ttlSeconds != HColumnDescriptor.DEFAULT_TTL) {
      // minVersions only comes into play with a TTL:
      if (minVersions != HColumnDescriptor.DEFAULT_MIN_VERSIONS) {
        buffer.append(String.format("(age() > %s && version() > %s)", bigtableTtl, minVersions));
      } else {
        buffer.append(String.format("(age() > %s)", bigtableTtl));
      }
    }
    // Since the HBase version default is 1, which may or may not be the same as Bigtable
    if (buffer.length() != 0) {
      buffer.append(" || ");
    }
    buffer.append(String.format("(version() > %s)", maxVersions));

    return buffer.toString();
  }

  static Splitter gcExpressionOrSplitter = Splitter.on("|").trimResults().omitEmptyStrings();
  static Splitter gcExpressionAndSplitter = Splitter.on("&").trimResults().omitEmptyStrings();

  /**
   * Parse a Bigtable GC-Expression that is in line with
   * {@link #buildGarbageCollectionExpression(HColumnDescriptor)} into a the provided
   * {@link HColumnDescriptor}.  This method will likely throw IllegalStateException if the 
   * GC Expression isn't similar to buildGarbageCollectionExpression's expression.
   */
  public static void convertGarbageCollectionExpression(String gcExpression,
      HColumnDescriptor columnDescriptor) {
    String maxVersionExpression = null;
    String minVersionExpression = null;
    String ttlExpression = null;
    if (gcExpression.contains("||")) {
      for (String expression : gcExpressionOrSplitter.split(gcExpression)) {
        if (expression.contains("age()")) {
          for (String expressionComponent : gcExpressionAndSplitter.split(expression)) {
            if (expressionComponent.contains("age()")
                && expressionComponent.contains(">")) {
              ttlExpression = expressionComponent.replaceAll("[()]", "");
            } else if (expressionComponent.contains("version()")
                && expressionComponent.contains(">")) {
              minVersionExpression = expressionComponent.replaceAll("[()]", "");
            } else {
              throw new IllegalStateException(String.format(
                "Expression: '%s' could not be parsed.", expression));
            }
          }
        } else if (expression.contains("version()") && expression.contains(">")) {
          maxVersionExpression = expression.replaceAll("[()]", "");
        } else {
          throw new IllegalStateException(String.format(
            "Expression: '%s' could not be parsed.", expression));
        }
      }
    } else if (gcExpression.contains("version()") && gcExpression.contains(">")) {
      maxVersionExpression = gcExpression.replaceAll("[()]", "");
    } else {
      throw new IllegalStateException(String.format(
        "Expression: '%s' could not be parsed.", maxVersionExpression));
    }

    int maxVersions = getInteger(maxVersionExpression);
    columnDescriptor.setMaxVersions(maxVersions);

    if (minVersionExpression != null) {
      int minVersions = getInteger(minVersionExpression);
      Preconditions.checkState(minVersions < maxVersions,
          "HColumnDescriptor min versions must be less than max versions.");
      columnDescriptor.setMinVersions(minVersions);
    }
    if (ttlExpression != null) {
      long bigtableTtl = getLong(ttlExpression);
      int ttlSeconds =
          (int) TimeUnit.SECONDS.convert(bigtableTtl, BigtableConstants.BIGTABLE_TIMEUNIT);
      if (ttlSeconds != HColumnDescriptor.DEFAULT_TTL) {
        columnDescriptor.setTimeToLive(ttlSeconds);
      }
    }
  }

  private static Integer getInteger(String expression) {
    return Integer.valueOf(getNumber(expression));
  }

  private static Long getLong(String expression) {
    return Long.valueOf(getNumber(expression));
  }

  public static String getNumber(String expression) {
    return expression.replaceFirst(".*> *(\\d+).*", "$1");
  }

  /**
   * <p>Adapt a single instance of an HBase {@link HColumnDescriptor} to
   * an instance of {@link com.google.bigtable.admin.table.v1.ColumnFamily.Builder}.</p>
   *
   * <p>NOTE: This method does not set the name of the ColumnFamily.Builder.  The assumption is
   * that the CreateTableRequest or CreateColumFamilyRequest takes care of the naming.  As of now
   * (3/11/2015), the server insists on having a blank name.</p>
   */
  public ColumnFamily.Builder adapt(HColumnDescriptor columnDescriptor) {
    throwIfRequestingUnknownFeatures(columnDescriptor);
    throwIfRequestingUnsupportedFeatures(columnDescriptor);

    ColumnFamily.Builder resultBuilder = ColumnFamily.newBuilder();
    String gcExpression = buildGarbageCollectionExpression(columnDescriptor);
    if (!Strings.isNullOrEmpty(gcExpression)) {
      resultBuilder.setGcExpression(gcExpression);
    }
    return resultBuilder;
  }

  public HColumnDescriptor adapt(String familyName, ColumnFamily columnFamily) {
    HColumnDescriptor hColumnDescriptor = new HColumnDescriptor(familyName);
    String gcExpression = columnFamily.getGcExpression();
    if (gcExpression != null) {
      convertGarbageCollectionExpression(gcExpression, hColumnDescriptor);
    }
    return hColumnDescriptor;
  }
}
