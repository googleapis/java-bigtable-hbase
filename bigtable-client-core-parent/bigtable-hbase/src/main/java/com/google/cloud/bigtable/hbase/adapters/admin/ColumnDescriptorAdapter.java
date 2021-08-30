/*
 * Copyright 2015 Google LLC
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.google.cloud.bigtable.hbase.adapters.admin;

import static com.google.cloud.bigtable.admin.v2.models.GCRules.GCRULES;

import com.google.api.core.InternalApi;
import com.google.bigtable.admin.v2.ColumnFamily;
import com.google.bigtable.admin.v2.GcRule;
import com.google.bigtable.admin.v2.GcRule.RuleCase;
import com.google.cloud.bigtable.admin.v2.models.GCRules.GCRule;
import com.google.common.base.Joiner;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HConstants;
import org.threeten.bp.Duration;

/**
 * Adapt a single instance of an HBase {@link org.apache.hadoop.hbase.HColumnDescriptor} to an
 * instance of {@link com.google.bigtable.admin.v2.ColumnFamily}
 *
 * <p>For internal use only - public for technical reasons.
 */
@InternalApi("For internal usage only")
public class ColumnDescriptorAdapter {

  public static final ColumnDescriptorAdapter INSTANCE = new ColumnDescriptorAdapter();

  /**
   * Configuration keys that we can support unconditionally and we provide a mapped version of the
   * column descriptor to Bigtable.
   */
  public static final Set<String> SUPPORTED_OPTION_KEYS =
      ImmutableSet.of(HColumnDescriptor.MIN_VERSIONS, HColumnDescriptor.TTL, HConstants.VERSIONS);

  /** Configuration keys that we ignore unconditionally */
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
   * Configuration option values that we ignore as long as the value is the one specified below. Any
   * other value results in an exception.
   */
  public static final Map<String, String> SUPPORTED_OPTION_VALUES =
      ImmutableMap.of(
          HColumnDescriptor.KEEP_DELETED_CELLS, Boolean.toString(false),
          HColumnDescriptor.COMPRESS_TAGS, Boolean.toString(false));

  /**
   * Build a list of configuration keys that we don't know how to handle
   *
   * @param columnDescriptor a {@link org.apache.hadoop.hbase.HColumnDescriptor} object.
   * @return a {@link java.util.List} object.
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
   *
   * @param columnDescriptor a {@link org.apache.hadoop.hbase.HColumnDescriptor} object.
   * @return a {@link java.util.Map} object.
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
   * Throw an {@link java.lang.UnsupportedOperationException} if the column descriptor cannot be
   * adapted due to it having unknown configuration keys.
   *
   * @param columnDescriptor a {@link org.apache.hadoop.hbase.HColumnDescriptor} object.
   */
  public static void throwIfRequestingUnknownFeatures(HColumnDescriptor columnDescriptor) {
    List<String> unknownFeatures = getUnknownFeatures(columnDescriptor);
    if (!unknownFeatures.isEmpty()) {
      String featureString =
          String.format(
              "Unknown configuration options: [%s]", Joiner.on(", ").join(unknownFeatures));
      throw new UnsupportedOperationException(featureString);
    }
  }

  /**
   * Throw an {@link java.lang.UnsupportedOperationException} if the column descriptor cannot be
   * adapted due to it having configuration values that are not supported.
   *
   * @param columnDescriptor a {@link org.apache.hadoop.hbase.HColumnDescriptor} object.
   */
  public static void throwIfRequestingUnsupportedFeatures(HColumnDescriptor columnDescriptor) {
    Map<String, String> unsupportedConfiguration = getUnsupportedFeatures(columnDescriptor);
    if (!unsupportedConfiguration.isEmpty()) {
      List<String> configurationStrings = new ArrayList<String>(unsupportedConfiguration.size());
      for (Map.Entry<String, String> entry : unsupportedConfiguration.entrySet()) {
        configurationStrings.add(String.format("(%s: %s)", entry.getKey(), entry.getValue()));
      }

      String exceptionMessage =
          String.format(
              "Unsupported configuration options: %s", Joiner.on(",").join(configurationStrings));
      throw new UnsupportedOperationException(exceptionMessage);
    }
  }

  /**
   * Construct an Bigtable {@link GCRule} from the given column descriptor.
   *
   * @param columnDescriptor a {@link org.apache.hadoop.hbase.HColumnDescriptor} object.
   * @return a {@link GCRule} object.
   */
  public static GCRule buildGarbageCollectionRule(HColumnDescriptor columnDescriptor) {
    int maxVersions = columnDescriptor.getMaxVersions();
    int minVersions = columnDescriptor.getMinVersions();
    int ttlSeconds = columnDescriptor.getTimeToLive();

    Preconditions.checkState(
        minVersions < maxVersions,
        "HColumnDescriptor min versions must be less than max versions.");

    if (ttlSeconds == HColumnDescriptor.DEFAULT_TTL) {
      if (maxVersions == Integer.MAX_VALUE) {
        return null;
      } else {
        return GCRULES.maxVersions(maxVersions);
      }
    }

    // minVersions only comes into play with a TTL:
    GCRule ageRule = GCRULES.maxAge(Duration.ofSeconds(ttlSeconds));
    if (minVersions != HColumnDescriptor.DEFAULT_MIN_VERSIONS) {
      // The logic here is: only delete a cell if:
      //  1) the age is older than :ttlSeconds AND
      //  2) the cell's relative version number is greater than :minVersions
      //
      // Bigtable's nomenclature for this is:
      // Intersection (AND)
      //    - maxAge = :HBase_ttlSeconds
      //    - maxVersions = :HBase_minVersion
      ageRule = GCRULES.intersection().rule(ageRule).rule(GCRULES.maxVersions(minVersions));
    }
    if (maxVersions == Integer.MAX_VALUE) {
      return ageRule;
    } else {
      return GCRULES.union().rule(ageRule).rule(GCRULES.maxVersions(maxVersions));
    }
  }

  /**
   * Parse a Bigtable {@link GcRule} that is in line with {@link
   * #buildGarbageCollectionRule(HColumnDescriptor)} into the provided {@link HColumnDescriptor}.
   *
   * <p>This method will likely throw IllegalStateException or IllegalArgumentException if the GC
   * Rule isn't similar to {@link #buildGarbageCollectionRule(HColumnDescriptor)}'s rule.
   */
  private static void convertGarbageCollectionRule(
      GcRule gcRule, HColumnDescriptor columnDescriptor) {

    // The Bigtable default is to have infinite versions.
    columnDescriptor.setMaxVersions(Integer.MAX_VALUE);
    if (gcRule == null || gcRule.equals(GcRule.getDefaultInstance())) {
      return;
    }
    switch (gcRule.getRuleCase()) {
      case MAX_AGE:
        columnDescriptor.setTimeToLive((int) gcRule.getMaxAge().getSeconds());
        return;
      case MAX_NUM_VERSIONS:
        columnDescriptor.setMaxVersions(gcRule.getMaxNumVersions());
        return;
      case INTERSECTION:
        {
          // minVersions and maxAge are set.
          processIntersection(gcRule, columnDescriptor);
          return;
        }
      case UNION:
        {
          // (minVersion && maxAge) || maxVersions
          List<GcRule> unionRules = gcRule.getUnion().getRulesList();
          Preconditions.checkArgument(unionRules.size() == 2, "Cannot process rule " + gcRule);
          if (hasRule(unionRules, RuleCase.INTERSECTION)) {
            processIntersection(getRule(unionRules, RuleCase.INTERSECTION), columnDescriptor);
          } else {
            columnDescriptor.setTimeToLive(
                (int) getRule(unionRules, RuleCase.MAX_AGE).getMaxAge().getSeconds());
          }
          columnDescriptor.setMaxVersions(getVersionCount(unionRules));
          return;
        }
      default:
        throw new IllegalArgumentException("Could not proess gc rules: " + gcRule);
    }
  }

  /**
   * processIntersection.
   *
   * @param gcRule a {@link com.google.bigtable.admin.v2.GcRule} object.
   * @param columnDescriptor a {@link org.apache.hadoop.hbase.HColumnDescriptor} object.
   */
  protected static void processIntersection(GcRule gcRule, HColumnDescriptor columnDescriptor) {
    // minVersions and maxAge are set.
    List<GcRule> intersectionRules = gcRule.getIntersection().getRulesList();
    Preconditions.checkArgument(intersectionRules.size() == 2, "Cannot process rule " + gcRule);
    columnDescriptor.setMinVersions(getVersionCount(intersectionRules));
    columnDescriptor.setTimeToLive(getTtl(intersectionRules));
  }

  private static int getVersionCount(List<GcRule> intersectionRules) {
    return getRule(intersectionRules, RuleCase.MAX_NUM_VERSIONS).getMaxNumVersions();
  }

  private static int getTtl(List<GcRule> intersectionRules) {
    return (int) getRule(intersectionRules, RuleCase.MAX_AGE).getMaxAge().getSeconds();
  }

  private static GcRule getRule(List<GcRule> intersectionRules, RuleCase ruleCase) {
    for (GcRule gcRule : intersectionRules) {
      if (gcRule.getRuleCase() == ruleCase) {
        return gcRule;
      }
    }
    throw new IllegalStateException("Could not get process: " + intersectionRules);
  }

  private static boolean hasRule(List<GcRule> intersectionRules, RuleCase ruleCase) {
    for (GcRule gcRule : intersectionRules) {
      if (gcRule.getRuleCase() == ruleCase) {
        return true;
      }
    }
    return false;
  }

  /**
   * Adapt a single instance of an HBase {@link org.apache.hadoop.hbase.HColumnDescriptor} to an
   * instance of {@link com.google.bigtable.admin.v2.ColumnFamily.Builder}.
   *
   * <p>NOTE: This method does not set the name of the ColumnFamily.Builder. The assumption is that
   * the CreateTableRequest or CreateColumnFamilyRequest takes care of the naming. As of now
   * (3/11/2015), the server insists on having a blank name.
   *
   * @param columnDescriptor a {@link org.apache.hadoop.hbase.HColumnDescriptor} object.
   * @return a {@link com.google.bigtable.admin.v2.ColumnFamily.Builder} object.
   */
  public ColumnFamily adapt(HColumnDescriptor columnDescriptor) {
    throwIfRequestingUnknownFeatures(columnDescriptor);
    throwIfRequestingUnsupportedFeatures(columnDescriptor);

    ColumnFamily.Builder resultBuilder = ColumnFamily.newBuilder();
    GCRule gcRule = buildGarbageCollectionRule(columnDescriptor);
    if (gcRule != null) {
      resultBuilder.setGcRule(gcRule.toProto());
    }
    return resultBuilder.build();
  }

  /**
   * Convert a Bigtable {@link com.google.cloud.bigtable.admin.v2.models.ColumnFamily} to an HBase
   * {@link HColumnDescriptor}. See {@link #convertGarbageCollectionRule(GcRule, HColumnDescriptor)}
   * for more info.
   *
   * @param columnFamily a {@link com.google.cloud.bigtable.admin.v2.models.ColumnFamily} object.
   * @return a {@link org.apache.hadoop.hbase.HColumnDescriptor} object.
   */
  public HColumnDescriptor adapt(
      com.google.cloud.bigtable.admin.v2.models.ColumnFamily columnFamily) {
    HColumnDescriptor hColumnDescriptor = new HColumnDescriptor(columnFamily.getId());
    GCRule gcRule = columnFamily.getGCRule();
    if (gcRule != null) {
      convertGarbageCollectionRule(gcRule.toProto(), hColumnDescriptor);
    }
    return hColumnDescriptor;
  }
}
