/*
 * Copyright 2015 Google Inc. All Rights Reserved.
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
package com.google.cloud.bigtable.hbase2_x.adapters.admin;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.client.ColumnFamilyDescriptor;
import com.google.bigtable.admin.v2.ColumnFamily;
import com.google.bigtable.admin.v2.GcRule;
import com.google.bigtable.admin.v2.GcRule.Intersection;
import com.google.bigtable.admin.v2.GcRule.Union;
import com.google.bigtable.admin.v2.GcRule.Intersection.Builder;
import com.google.cloud.bigtable.hbase.adapters.admin.ColumnDescriptorAdapter;
import com.google.common.base.Joiner;
import com.google.common.base.Preconditions;
import com.google.protobuf.Duration;

/**
 * Temporary copy to get things going. Cannot use existing adopter due to binary incompatibility.
 * Needs a better re-factoring 
 * 
 * @author spollapally
 */
public class ColumnDescriptorAdapter2x extends ColumnDescriptorAdapter {

  public ColumnDescriptorAdapter2x() {
    super();
  }

  public ColumnFamily.Builder adapt(ColumnFamilyDescriptor columnDescriptor) {

    throwIfRequestingUnknownFeatures2x(columnDescriptor);
    throwIfRequestingUnsupportedFeatures2x(columnDescriptor);

    ColumnFamily.Builder resultBuilder = ColumnFamily.newBuilder();
    GcRule gcRule = buildGarbageCollectionRule2x(columnDescriptor);
    if (gcRule != null) {
      resultBuilder.setGcRule(gcRule);
    }
    return resultBuilder;
  }

  public static void throwIfRequestingUnknownFeatures2x(ColumnFamilyDescriptor columnDescriptor) {
    List<String> unknownFeatures = getUnknownFeatures2x(columnDescriptor);
    if (!unknownFeatures.isEmpty()) {
      String featureString = String.format("Unknown configuration options: [%s]",
          Joiner.on(", ").join(unknownFeatures));
      throw new UnsupportedOperationException(featureString);
    }
  }

  /**
   * Throw an {@link java.lang.UnsupportedOperationException} if the column descriptor cannot be
   * adapted due to it having configuration values that are not supported.
   *
   * @param columnDescriptor a {@link org.apache.hadoop.hbase.HColumnDescriptor} object.
   */
  public static void throwIfRequestingUnsupportedFeatures2x(
      ColumnFamilyDescriptor columnDescriptor) {
    Map<String, String> unsupportedConfiguration = getUnsupportedFeatures2x(columnDescriptor);
    if (!unsupportedConfiguration.isEmpty()) {
      List<String> configurationStrings = new ArrayList<String>(unsupportedConfiguration.size());
      for (Map.Entry<String, String> entry : unsupportedConfiguration.entrySet()) {
        configurationStrings.add(String.format("(%s: %s)", entry.getKey(), entry.getValue()));
      }

      String exceptionMessage = String.format("Unsupported configuration options: %s",
          Joiner.on(",").join(configurationStrings));
      throw new UnsupportedOperationException(exceptionMessage);
    }
  }

  public static Map<String, String> getUnsupportedFeatures2x(
      ColumnFamilyDescriptor columnDescriptor) {
    Map<String, String> unsupportedConfiguration = new HashMap<String, String>();

    Map<String, String> configuration = columnDescriptor.getConfiguration();
    for (Map.Entry<String, String> entry : SUPPORTED_OPTION_VALUES.entrySet()) {
      if (configuration.containsKey(entry.getKey()) && configuration.get(entry.getKey()) != null
          && !entry.getValue().equals(configuration.get(entry.getKey()))) {
        unsupportedConfiguration.put(entry.getKey(), configuration.get(entry.getKey()));
      }
    }
    return unsupportedConfiguration;
  }

  /**
   * Build a list of configuration keys that we don't know how to handle
   *
   * @param columnDescriptor a {@link org.apache.hadoop.hbase.HColumnDescriptor} object.
   * @return a {@link java.util.List} object.
   */
  public static List<String> getUnknownFeatures2x(ColumnFamilyDescriptor columnDescriptor) {
    List<String> unknownFeatures = new ArrayList<String>();
    for (Map.Entry<String, String> entry : columnDescriptor.getConfiguration().entrySet()) {
      String key = entry.getKey();
      if (!SUPPORTED_OPTION_KEYS.contains(key) && !IGNORED_OPTION_KEYS.contains(key)
          && !SUPPORTED_OPTION_VALUES.containsKey(key)) {
        unknownFeatures.add(key);
      }
    }
    return unknownFeatures;
  }

  /**
   * Construct an Bigtable {@link com.google.bigtable.admin.v2.GcRule} from the given column
   * descriptor.
   *
   * @param columnDescriptor a {@link org.apache.hadoop.hbase.HColumnDescriptor} object.
   * @return a {@link com.google.bigtable.admin.v2.GcRule} object.
   */
  public static GcRule buildGarbageCollectionRule2x(ColumnFamilyDescriptor columnDescriptor) {
    int maxVersions = columnDescriptor.getMaxVersions();
    int minVersions = columnDescriptor.getMinVersions();
    int ttlSeconds = columnDescriptor.getTimeToLive();

    Preconditions.checkState(minVersions < maxVersions,
        "HColumnDescriptor min versions must be less than max versions.");

    if (ttlSeconds ==  HColumnDescriptor.DEFAULT_TTL) {
      if (maxVersions == Integer.MAX_VALUE) {
        return null;
      } else {
        return createMaxVersionsRule2x(maxVersions);
      }
    }

    // minVersions only comes into play with a TTL:
    GcRule ageRule =
        GcRule.newBuilder().setMaxAge(Duration.newBuilder().setSeconds(ttlSeconds)).build();
    if (minVersions != HColumnDescriptor.DEFAULT_MIN_VERSIONS) {
      // The logic here is: only delete a cell if:
      // 1) the age is older than :ttlSeconds AND
      // 2) the cell's relative version number is greater than :minVersions
      //
      // Bigtable's nomenclature for this is:
      // Intersection (AND)
      // - maxAge = :HBase_ttlSeconds
      // - maxVersions = :HBase_minVersion
      GcRule minVersionRule = createMaxVersionsRule2x(minVersions);
      Builder ageAndMin = Intersection.newBuilder().addRules(ageRule).addRules(minVersionRule);
      ageRule = GcRule.newBuilder().setIntersection(ageAndMin).build();
    }
    if (maxVersions == Integer.MAX_VALUE) {
      return ageRule;
    } else {
      GcRule maxVersionsRule = createMaxVersionsRule2x(maxVersions);
      Union ageAndMax = Union.newBuilder().addRules(ageRule).addRules(maxVersionsRule).build();
      return GcRule.newBuilder().setUnion(ageAndMax).build();
    }
  }

  private static GcRule createMaxVersionsRule2x(int maxVersions) {
    return GcRule.newBuilder().setMaxNumVersions(maxVersions).build();
  }

}
