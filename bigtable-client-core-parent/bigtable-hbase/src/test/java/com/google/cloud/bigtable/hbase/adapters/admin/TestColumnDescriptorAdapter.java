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

import com.google.bigtable.admin.v2.ColumnFamily;
import com.google.cloud.bigtable.admin.v2.models.GCRules;
import com.google.cloud.bigtable.admin.v2.models.GCRules.GCRule;
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
import org.threeten.bp.Duration;

/** Tests for {@link ColumnDescriptorAdapter}. */
@RunWith(JUnit4.class)
public class TestColumnDescriptorAdapter {

  private static final String FAMILY_NAME = "testFamily";
  private ColumnDescriptorAdapter adapter;
  private HColumnDescriptor descriptor;

  @Before
  public void setup() {
    adapter = new ColumnDescriptorAdapter();
    descriptor = new HColumnDescriptor(FAMILY_NAME);
  }

  @Rule public ExpectedException expectedException = ExpectedException.none();

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

    Assert.assertArrayEquals(new byte[0], result.build().toByteArray());
  }

  @Test
  public void ttlIsPreservedInGcRule() {
    // TTL of 1 day (in seconds):
    int ttl = 86400;
    descriptor.setTimeToLive(ttl);
    ColumnFamily result = adapter.adapt(descriptor);
    GCRules.GCRule expected =
        GCRULES.union().rule(GCRULES.maxAge(Duration.ofSeconds(ttl))).rule(GCRULES.maxVersions(1));
    Assert.assertEquals(expected.toProto(), result.getGcRule());
  }

  @Test
  public void ttlIsPreservedInColumnFamily() {
    // TTL of 1 day (in microseconds):
    GCRules.GCRule expected =
        GCRULES
            .union()
            .rule(GCRULES.maxAge(Duration.ofSeconds(86400)))
            .rule(GCRULES.maxVersions(1));
    HColumnDescriptor descriptor = adapter.adapt(columnFamily(expected));
    Assert.assertEquals(1, descriptor.getMaxVersions());
    Assert.assertEquals(86400, descriptor.getTimeToLive());
  }

  @Test
  public void maxVersionsIsPreservedInGcExpression() {
    descriptor.setMaxVersions(10);
    ColumnFamily result = adapter.adapt(descriptor);
    GCRules.GCRule expected = GCRULES.maxVersions(10);
    Assert.assertEquals(expected.toProto(), result.getGcRule());
  }

  @Test
  public void maxVersionsIsPreservedInColumnFamily() {
    GCRules.GCRule expected = GCRULES.maxVersions(10);
    HColumnDescriptor descriptor = adapter.adapt(columnFamily(expected));
    Assert.assertEquals(10, descriptor.getMaxVersions());
  }

  @Test
  public void minMaxTtlInDescriptor() {
    descriptor.setMaxVersions(20);
    descriptor.setMinVersions(10);
    descriptor.setTimeToLive(86400); // 1 day in seconds
    ColumnFamily result = adapter.adapt(descriptor);
    Assert.assertEquals(minMaxRule(10, 86400, 20).toProto(), result.getGcRule());
  }

  @Test
  public void minMaxTtlInColumnFamily() {
    HColumnDescriptor descriptor = adapter.adapt(columnFamily(minMaxRule(10, 86400, 20)));
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
    adapter.adapt(columnFamily(minMaxRule(20, 86400, 10)));
  }

  @Test
  public void testBlankExpression() {
    com.google.cloud.bigtable.admin.v2.models.ColumnFamily columnFamily =
        com.google.cloud.bigtable.admin.v2.models.ColumnFamily.fromProto(
            "family", ColumnFamily.getDefaultInstance());
    HColumnDescriptor descriptor = adapter.adapt(columnFamily);
    Assert.assertEquals(Integer.MAX_VALUE, descriptor.getMaxVersions());
    Assert.assertEquals(null, ColumnDescriptorAdapter.buildGarbageCollectionRule(descriptor));
  }

  @Test
  public void testGCruleMaxVersion() {
    int ttl = 100;
    descriptor.setTimeToLive(ttl);
    descriptor.setMaxVersions(Integer.MAX_VALUE);
    ColumnFamily result = adapter.adapt(descriptor);
    GCRule expected = GCRULES.maxAge(Duration.ofSeconds(ttl));
    Assert.assertEquals(expected.toProto(), result.getGcRule());
  }

  @Test
  public void testAdaptWithColumnFamilyForMaxAge() {
    int ttl = 86400;
    GCRule maxAgeGCRule = GCRULES.maxAge(Duration.ofSeconds(ttl));
    HColumnDescriptor actual = adapter.adapt(columnFamily(maxAgeGCRule));
    Assert.assertEquals(ttl, actual.getTimeToLive());
  }

  // TODO: Remove this method and create ColumnFamily along with GCRule instead of using proto.
  private static com.google.cloud.bigtable.admin.v2.models.ColumnFamily columnFamily(GCRule rule) {
    return com.google.cloud.bigtable.admin.v2.models.ColumnFamily.fromProto(
        "family", ColumnFamily.newBuilder().setGcRule(rule.toProto()).build());
  }

  private GCRule minMaxRule(int minVersions, int ttl, int maxVersions) {
    GCRule intersection =
        GCRULES
            .intersection()
            .rule(GCRULES.maxAge(Duration.ofSeconds(ttl)))
            .rule(GCRULES.maxVersions(minVersions));
    return GCRULES.union().rule(intersection).rule(GCRULES.maxVersions(maxVersions));
  }
}
