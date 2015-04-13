package com.google.cloud.bigtable.hbase.adapters;

import com.google.bigtable.repackaged.com.google.protobuf.ByteString;
import com.google.bigtable.v1.ReadRowsRequest;
import com.google.bigtable.v1.RowFilter;
import com.google.bigtable.v1.RowFilter.Chain;
import com.google.bigtable.v1.RowFilter.Interleave;
import com.google.cloud.bigtable.hbase.DataGenerationHelper;

import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

import java.io.IOException;
import java.nio.charset.StandardCharsets;

/**
 * Unit tests for the {@link GetProtoAdapter}
 */
@RunWith(JUnit4.class)
public class TestGetProtoAdapter {

  private GetProtoAdapter getAdapter =
      new GetProtoAdapter(new ScanProtoAdapter(new FilterAdapter()));
  private DataGenerationHelper dataHelper = new DataGenerationHelper();

  private Get makeValidGet(byte[] rowKey) throws IOException {
    Get get = new Get(rowKey);
    get.setMaxVersions(Integer.MAX_VALUE);
    return get;
  }

  @Test
  public void rowKeyIsSetInRequest() throws IOException {
    Get get = makeValidGet(dataHelper.randomData("rk1"));
    ReadRowsRequest.Builder rowRequestBuilder = getAdapter.adapt(get);
    ByteString adaptedRowKey = rowRequestBuilder.getRowKey();
    Assert.assertEquals(
        new String(get.getRow(), StandardCharsets.UTF_8),
        adaptedRowKey.toStringUtf8());
  }

  @Test
  public void maxVersionsIsSet() throws IOException {
    Get get = makeValidGet(dataHelper.randomData("rk1"));
    get.setMaxVersions(10);
    ReadRowsRequest.Builder rowRequestBuilder = getAdapter.adapt(get);
    Assert.assertEquals(
        Chain.newBuilder()
            .addFilters(RowFilter.newBuilder()
                .setFamilyNameRegexFilter(".*"))
            .addFilters(RowFilter.newBuilder()
                .setCellsPerColumnLimitFilter(10))
            .build(),
        rowRequestBuilder.getFilter().getChain());
  }

  @Test
  public void columnFamilyIsSet() throws IOException {
    Get get = makeValidGet(dataHelper.randomData("rk1"));
    get.addFamily(Bytes.toBytes("f1"));
    ReadRowsRequest.Builder rowRequestBuilder = getAdapter.adapt(get);
    Assert.assertEquals(
        Chain.newBuilder()
            .addFilters(RowFilter.newBuilder()
                .setFamilyNameRegexFilter("f1"))
            .addFilters(RowFilter.newBuilder()
                .setCellsPerColumnLimitFilter(Integer.MAX_VALUE))
            .build(),
        rowRequestBuilder.getFilter().getChain());
  }

  @Test
  public void columnQualifierIsSet() throws IOException {
    Get get = makeValidGet(dataHelper.randomData("rk1"));
    get.addColumn(Bytes.toBytes("f1"), Bytes.toBytes("q1"));
    ReadRowsRequest.Builder rowRequestBuilder = getAdapter.adapt(get);
    Assert.assertEquals(
        Chain.newBuilder()
            .addFilters(
                RowFilter.newBuilder().setChain(Chain.newBuilder()
                    .addFilters(RowFilter.newBuilder()
                        .setFamilyNameRegexFilter("f1"))
                    .addFilters(RowFilter.newBuilder()
                        .setColumnQualifierRegexFilter(ByteString.copyFromUtf8("q1")))))
            .addFilters(RowFilter.newBuilder()
                .setCellsPerColumnLimitFilter(Integer.MAX_VALUE))
            .build(),
        rowRequestBuilder.getFilter().getChain());
  }

  @Test
  public void multipleQualifiersAreSet() throws IOException {
    Get get = makeValidGet(dataHelper.randomData("rk1"));
    get.addColumn(Bytes.toBytes("f1"), Bytes.toBytes("q1"));
    get.addColumn(Bytes.toBytes("f1"), Bytes.toBytes("q2"));
    ReadRowsRequest.Builder rowRequestBuilder = getAdapter.adapt(get);
    Assert.assertEquals(
        Chain.newBuilder()
            .addFilters(
                RowFilter.newBuilder().setChain(Chain.newBuilder()
                    .addFilters(RowFilter.newBuilder()
                        .setFamilyNameRegexFilter("f1"))
                    .addFilters(RowFilter.newBuilder()
                        .setInterleave(
                            Interleave.newBuilder()
                                .addFilters(RowFilter.newBuilder()
                                    .setColumnQualifierRegexFilter(
                                        ByteString.copyFromUtf8("q1")))
                                .addFilters(RowFilter.newBuilder()
                                    .setColumnQualifierRegexFilter(
                                        ByteString.copyFromUtf8("q2")))))))
            .addFilters(RowFilter.newBuilder()
                .setCellsPerColumnLimitFilter(Integer.MAX_VALUE))
            .build(),
        rowRequestBuilder.getFilter().getChain());
  }
}
