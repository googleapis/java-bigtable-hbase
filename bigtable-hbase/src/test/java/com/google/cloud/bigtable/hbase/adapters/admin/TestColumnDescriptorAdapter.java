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
package com.google.cloud.bigtable.hbase.adapters.admin;

import java.util.Arrays;
import java.util.Map;
import java.util.Set;

import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.io.compress.Compression;
import org.apache.hadoop.hbase.io.encoding.DataBlockEncoding;
import org.apache.hadoop.hbase.regionserver.BloomType;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

import com.google.bigtable.admin.table.v1.ColumnFamily;
import com.google.bigtable.admin.table.v1.GcRule;
import com.google.bigtable.admin.table.v1.GcRule.Intersection;
import com.google.bigtable.admin.table.v1.GcRule.Union;
import com.google.cloud.bigtable.hbase.adapters.admin.ColumnDescriptorAdapter;
import com.google.protobuf.Duration;

/**
 * Tests for {@link ColumnDescriptorAdapter}.
 */
@RunWith(JUnit4.class)
public class TestColumnDescriptorAdapter {

  private ColumnDescriptorAdapter adapter;
  private HColumnDescriptor descriptor;

  @Before
  public void setup() {
    adapter = new ColumnDescriptorAdapter();
    descriptor = new HColumnDescriptor("testFamily");
  }

  @Rule
  public ExpectedException expectedException = ExpectedException.none();

  @Test
  public void exceptionIsThrownOnUnknownOptions() {
    descriptor.setConfiguration("UnknownConfigurationKey", "UnknownConfigurationValue");

    expectedException.expectMessage("Unknown configuration options");
    expectedException.expectMessage("UnknownConfigurationKey");
    expectedException.expect(UnsupportedOperationException.class);

    adapter.adapt(descriptor);
  }

  @Test
  public void exceptionIsThrownWhenAUnsupportedValueIsSet() {
    Set<Map.Entry<String, String>> ignoredEntryes =
        ColumnDescriptorAdapter.SUPPORTED_OPTION_VALUES.entrySet();

    for (Map.Entry<String, String> entry : ignoredEntryes) {
      String invalidValue = entry.getValue() + "_invalid";
      descriptor.setConfiguration(entry.getKey(), invalidValue);

      expectedException.expectMessage(entry.getKey());
      expectedException.expectMessage(invalidValue);
    }

    expectedException.expectMessage("Unsupported configuration options");
    expectedException.expect(UnsupportedOperationException.class);
    adapter.adapt(descriptor);
  }

  @Test
  public void ignoredOptionsAreIgnored() {
    // We're really checking to make certain we don't trigger an exception for an ignored option:
    descriptor.setCompressionType(Compression.Algorithm.LZ4);
    descriptor.setCompactionCompressionType(Compression.Algorithm.LZ4);
    descriptor.setDataBlockEncoding(DataBlockEncoding.FAST_DIFF);
    descriptor.setBlockCacheEnabled(false);
    descriptor.setCacheDataOnWrite(true);
    descriptor.setCacheDataInL1(true);
    descriptor.setEvictBlocksOnClose(false);
    descriptor.setBloomFilterType(BloomType.ROW);
    descriptor.setPrefetchBlocksOnOpen(true);
    descriptor.setBlocksize(16 * 1024);
    descriptor.setScope(1); // REPLICATION_SCOPE
    descriptor.setInMemory(true);

    ColumnFamily.Builder result = adapter.adapt(descriptor)
        .clearName()
        .clearGcRule();

    Assert.assertArrayEquals(
        new byte[0],
        result.build().toByteArray());
  }

  @Test
  @Ignore("The name of the column family is in a higher level object like Table or the create column request")
  public void columnFamilyNameIsPreserved() {
    String adapted = adapter.adapt(descriptor).getName();
    Assert.assertTrue(adapted.endsWith(descriptor.getNameAsString()));
  }

  @Test
  public void ttlIsPreservedInGcRule() {
    // TTL of 1 day (in seconds):
    int ttl = 86400;
    descriptor.setTimeToLive(ttl);
    ColumnFamily.Builder result = adapter.adapt(descriptor);
    Assert.assertEquals(maxVersionsAndTtl(1, ttl), result.getGcRule());
  }

  @Test
  public void ttlIsPreservedInColumnFamily() {
    // TTL of 1 day (in microseconds):
    HColumnDescriptor descriptor =
        adapter.adapt("family", columnFamily(union(maxAgeRule(86400), maxNumVersionsRule(1))));
    Assert.assertEquals(1, descriptor.getMaxVersions());
    Assert.assertEquals(86400, descriptor.getTimeToLive());
  }

  @Test
  public void maxVersionsIsPreservedInGcExpression() {
    descriptor.setMaxVersions(10);
    ColumnFamily.Builder result = adapter.adapt(descriptor);
    Assert.assertEquals(maxNumVersionsRule(10), result.getGcRule());
  }

  @Test
  public void maxVersionsIsPreservedInColumnFamily() {
    HColumnDescriptor descriptor = adapter.adapt("family", columnFamily(maxNumVersionsRule(10)));
    Assert.assertEquals(10, descriptor.getMaxVersions());
  }

  @Test
  public void minMaxTtlInDescriptor() {
    descriptor.setMaxVersions(20);
    descriptor.setMinVersions(10);
    descriptor.setTimeToLive(86400); // 1 day in seconds
    ColumnFamily.Builder result = adapter.adapt(descriptor);
    Assert.assertEquals(minMaxRule(10, 86400, 20), result.getGcRule());
  }

  @Test
  public void minMaxTtlInColumnFamily() {
    HColumnDescriptor descriptor = adapter.adapt("family", columnFamily(minMaxRule(10, 86400, 20)));
    Assert.assertEquals(20, descriptor.getMaxVersions());
    Assert.assertEquals(10, descriptor.getMinVersions());
    Assert.assertEquals(86400, descriptor.getTimeToLive());
  }

  @Test
  public void minVersionsMustBeLessThanMaxversion() {
    descriptor.setMaxVersions(10);
    descriptor.setMinVersions(20);
    expectedException.expect(IllegalStateException.class);
    adapter.adapt(descriptor);
  }

  @Test
  public void minVersionsMustBeLessThanMaxversionInExpression() {
    expectedException.expect(IllegalArgumentException.class);
    adapter.adapt("family", columnFamily(minMaxRule(20, 86400, 10)));
  }

  @Test
  public void testBlankExpression(){
    HColumnDescriptor descriptor = adapter.adapt("family", ColumnFamily.getDefaultInstance());
    Assert.assertEquals(Integer.MAX_VALUE, descriptor.getMaxVersions());
    Assert.assertEquals(null, ColumnDescriptorAdapter.buildGarbageCollectionRule(descriptor));
  }

  private static ColumnFamily columnFamily(GcRule rule) {
    return ColumnFamily.newBuilder().setGcRule(rule).build();
  }

  private static GcRule maxNumVersionsRule(int maxNumVersions) {
    return GcRule.newBuilder().setMaxNumVersions(maxNumVersions).build();
  }

  private static GcRule maxAgeRule(int ttlSecons) {
    return GcRule.newBuilder().setMaxAge(Duration.newBuilder().setSeconds(ttlSecons)).build();
  }

  private static GcRule maxVersionsAndTtl(int maxVersions, int ttl) {
    return union(maxAgeRule(ttl), maxNumVersionsRule(maxVersions));
  }

  private GcRule minMaxRule(int minVersions, int ttl, int maxVersions) {
    return union(
        intersection(
          maxAgeRule(ttl),
          maxNumVersionsRule(minVersions)
         ),
        maxNumVersionsRule(maxVersions));
  }

  private static GcRule union(GcRule... rules) {
    return GcRule.newBuilder().setUnion(Union.newBuilder().addAllRules(Arrays.asList(rules))).build();
  }

  private static GcRule intersection(GcRule... rules) {
    return GcRule.newBuilder()
        .setIntersection(Intersection.newBuilder().addAllRules(Arrays.asList(rules))).build();
  }
}
