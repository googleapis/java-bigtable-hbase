package com.google.cloud.anviltop.hbase.adapters;

import com.google.bigtable.anviltop.AnviltopData.RowAppend;
import com.google.bigtable.anviltop.AnviltopServiceMessages.AppendRowRequest;
import com.google.cloud.anviltop.hbase.DataGenerationHelper;
import com.google.protobuf.ByteString;
import org.apache.hadoop.hbase.client.Append;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

import java.io.IOException;
import java.util.List;

@RunWith(JUnit4.class)
public class TestAppendAdapter {
  protected AppendAdapter appendAdapter = new AppendAdapter();
  protected DataGenerationHelper dataHelper = new DataGenerationHelper();

  @Test
  public void testBasicRowKeyAppend() throws IOException {
    byte[] rowKey = dataHelper.randomData("rk1-");
    Append append = new Append(rowKey);
    AppendRowRequest request = appendAdapter.adapt(append).build();
    ByteString adaptedRowKey = request.getAppend().getRowKey();
    Assert.assertArrayEquals(rowKey, adaptedRowKey.toByteArray());
  }

  @Test
  public void testMultipleAppends() throws IOException {
    byte[] rowKey = dataHelper.randomData("rk1-");

    byte[] family1 = Bytes.toBytes("family1");
    byte[] qualifier1 = Bytes.toBytes("qualifier1");
    byte[] value1 = Bytes.toBytes("value1");

    byte[] family2 = Bytes.toBytes("family2");
    byte[] qualifier2 = Bytes.toBytes("qualifier2");
    byte[] value2 = Bytes.toBytes("value2");

    Append append = new Append(rowKey);
    append.add(family1, qualifier1, value1);
    append.add(family2, qualifier2, value2);

    AppendRowRequest request = appendAdapter.adapt(append).build();
    List<RowAppend.Append> appends = request.getAppend().getAppendsList();
    Assert.assertEquals(2, request.getAppend().getAppendsList().size());
    Assert.assertEquals("family1:qualifier1", appends.get(0).getColumnName().toStringUtf8());
    Assert.assertEquals("value1", appends.get(0).getValue().toStringUtf8());
    Assert.assertEquals("family2:qualifier2", appends.get(1).getColumnName().toStringUtf8());
    Assert.assertEquals("value2", appends.get(1).getValue().toStringUtf8());
  }
}
