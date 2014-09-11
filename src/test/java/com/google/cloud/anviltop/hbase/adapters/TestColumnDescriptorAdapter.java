package com.google.cloud.anviltop.hbase.adapters;

import com.google.bigtable.anviltop.AnviltopData;

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

import java.util.Map;
import java.util.Set;

/**
 * Tests for {@link ColumnDescriptorAdapter}.
 */
@RunWith(JUnit4.class)
public class TestColumnDescriptorAdapter {

  ColumnDescriptorAdapter adapter;
  HColumnDescriptor descriptor;

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

    AnviltopData.ColumnFamily.Builder result = adapter.adapt(descriptor)
        .clearName()
        .clearGcExpression();

    Assert.assertArrayEquals(
        new byte[0],
        result.build().toByteArray());
  }

  @Test
  public void columnFamilyNameIsPreserved() {
    Assert.assertEquals(descriptor.getNameAsString(), adapter.adapt(descriptor).getName());
  }

  @Test
  public void ttlIsPreservedInGcExpression() {
    // TTL of 1 day (in seconds):
    descriptor.setTimeToLive(86400);
    AnviltopData.ColumnFamilyOrBuilder result = adapter.adapt(descriptor);

    Assert.assertEquals("(age() > 86400000000) || (versions() > 1)", result.getGcExpression());
  }

  @Test
  public void maxVersionsIsPreservedInGcExpression() {
    descriptor.setMaxVersions(10);
    AnviltopData.ColumnFamilyOrBuilder result = adapter.adapt(descriptor);
    Assert.assertEquals("(versions() > 10)", result.getGcExpression());
  }

  @Test
  public void minAndMaxMayBeSpecifiedTogetherWIthTtl() {
    descriptor.setMaxVersions(20);
    descriptor.setMinVersions(10);
    descriptor.setTimeToLive(86400); // 1 day in seconds
    AnviltopData.ColumnFamilyOrBuilder result = adapter.adapt(descriptor);
    Assert.assertEquals(
        "(age() > 86400000000 && versions() > 10) || (versions() > 20)",
        result.getGcExpression());
  }

  @Test
  public void minVersionsMustBeLessThanMaxVersions() {
    descriptor.setMaxVersions(10);
    descriptor.setMinVersions(20);
    expectedException.expect(IllegalStateException.class);
    adapter.adapt(descriptor);
  }
}
