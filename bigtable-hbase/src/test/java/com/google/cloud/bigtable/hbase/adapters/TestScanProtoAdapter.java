package com.google.cloud.bigtable.hbase.adapters;

import com.google.bigtable.v1.ReadRowsRequest;
import com.google.bigtable.v1.ReadRowsRequest.TargetCase;
import com.google.bigtable.v1.RowFilter;
import com.google.bigtable.v1.RowFilter.Chain;
import com.google.cloud.bigtable.hbase.adapters.filters.FilterAdapter;

import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

import java.io.IOException;

/**
 * Lightweight tests for the ScanAdapter. Many of the methods, such as filter building are
 * already tested in {@link TestGetProtoAdapter}.
 */
@RunWith(JUnit4.class)
public class TestScanProtoAdapter {

  private ScanProtoAdapter scanAdapter = new ScanProtoAdapter(FilterAdapter.buildAdapter());

  @Test
  public void testStartAndEndKeysAreSet() {
    byte[] startKey = Bytes.toBytes("startKey");
    byte[] stopKey = Bytes.toBytes("stopKey");
    Scan scan = new Scan();
    scan.setStartRow(startKey);
    scan.setStopRow(stopKey);
    ReadRowsRequest.Builder request = scanAdapter.adapt(scan);
    Assert.assertEquals(TargetCase.ROW_RANGE, request.getTargetCase());
    Assert.assertArrayEquals(startKey, request.getRowRange().getStartKey().toByteArray());
    Assert.assertArrayEquals(stopKey, request.getRowRange().getEndKey().toByteArray());
  }

  @Test
  public void maxVersionsIsSet() throws IOException {
    Scan scan = new Scan();
    scan.setMaxVersions(10);
    ReadRowsRequest.Builder rowRequestBuilder = scanAdapter.adapt(scan);
    Assert.assertEquals(
        Chain.newBuilder()
            .addFilters(RowFilter.newBuilder()
                .setFamilyNameRegexFilter(".*"))
            .addFilters(RowFilter.newBuilder()
                .setCellsPerColumnLimitFilter(10))
            .build(),
        rowRequestBuilder.getFilter().getChain());
  }
}
