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

import static com.google.cloud.bigtable.hbase.adapters.admin.ColumnDescriptorAdapter.intersection;
import static com.google.cloud.bigtable.hbase.adapters.admin.ColumnDescriptorAdapter.maxAge;
import static com.google.cloud.bigtable.hbase.adapters.admin.ColumnDescriptorAdapter.maxVersions;
import static com.google.cloud.bigtable.hbase.adapters.admin.ColumnDescriptorAdapter.union;

import java.util.Map;
import java.util.Set;

import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.io.compress.Compression;
import org.apache.hadoop.hbase.io.encoding.DataBlockEncoding;
import org.apache.hadoop.hbase.regionserver.BloomType;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

import com.google.bigtable.admin.v2.ColumnFamily;
import com.google.bigtable.admin.v2.GcRule;

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

    ColumnFamily.Builder result = adapter.adapt(descriptor).toBuilder().clearGcRule();

    Assert.assertArrayEquals(
        new byte[0],
        result.build().toByteArray());
  }

  @Test
  public void ttlIsPreservedInGcRule() {
    // TTL of 1 day (in seconds):
    int ttl = 86400;
    descriptor.setTimeToLive(ttl);
    ColumnFamily result = adapter.adapt(descriptor);
    Assert.assertEquals(union(maxAge(ttl), maxVersions(1)), result.getGcRule());
  }

  @Test
  public void ttlIsPreservedInColumnFamily() {
    // TTL of 1 day (in microseconds):
    HColumnDescriptor descriptor =
        adapter.adapt("family", columnFamily(union(maxAge(86400), maxVersions(1))));
    Assert.assertEquals(1, descriptor.getMaxVersions());
    Assert.assertEquals(86400, descriptor.getTimeToLive());
  }

  @Test
  public void maxVersionsIsPreservedInGcExpression() {
    descriptor.setMaxVersions(10);
    ColumnFamily result = adapter.adapt(descriptor);
    Assert.assertEquals(maxVersions(10), result.getGcRule());
  }

  @Test
  public void maxVersionsIsPreservedInColumnFamily() {
    HColumnDescriptor descriptor = adapter.adapt("family", columnFamily(maxVersions(10)));
    Assert.assertEquals(10, descriptor.getMaxVersions());
  }

  @Test
  public void minMaxTtlInDescriptor() {
    descriptor.setMaxVersions(20);
    descriptor.setMinVersions(10);
    descriptor.setTimeToLive(86400); // 1 day in seconds
    ColumnFamily result = adapter.adapt(descriptor);
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

  private GcRule minMaxRule(int minVersions, int ttl, int maxVersions) {
    return union(
        intersection(
          maxAge(ttl),
          maxVersions(minVersions)
         ),
        maxVersions(maxVersions));
  }
}
