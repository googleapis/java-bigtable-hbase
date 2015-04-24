package com.google.cloud.bigtable.hbase.adapters.filters;


import com.google.bigtable.v1.RowFilter;
import com.google.protobuf.ByteString;

import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.PrefixFilter;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

import java.io.IOException;

/**
 * Tests for {@link PrefixFilterAdapter}
 */
@RunWith(JUnit4.class)
public class TestPrefixFilterAdapter {
  @Test
  public void testPrefixAddedAsRowRegex() throws IOException {
    PrefixFilterAdapter adapter = new PrefixFilterAdapter();
    String prefix = "Foobar";
    PrefixFilter filter = new PrefixFilter(Bytes.toBytes(prefix));
    Scan emptyScan = new Scan();
    FilterAdapterContext context = new FilterAdapterContext(emptyScan);

    byte[] prefixRegex = Bytes.toBytes(prefix + "\\C*");
    Assert.assertEquals(
        RowFilter.newBuilder()
            .setRowKeyRegexFilter(
                ByteString.copyFrom(prefixRegex))
            .build(),
        adapter.adapt(context, filter));
  }
}
