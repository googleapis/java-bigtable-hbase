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
package com.google.cloud.bigtable.grpc.scanner;

import java.io.IOException;
import java.util.Arrays;

import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

import com.google.bigtable.v2.ReadRowsRequest;
import com.google.bigtable.v2.RowRange;
import com.google.bigtable.v2.RowSet;
import com.google.protobuf.ByteString;

/**
 * Test for the {@link ResumingStreamingResultScanner}
 */
@RunWith(JUnit4.class)
public class ReadRowsRequestManagerTest {

  static final ByteString BLANK = ByteString.EMPTY;

  static FlatRow buildRow(String rowKey) {
    return FlatRow.newBuilder()
        .withRowKey(ByteString.copyFromUtf8(rowKey))
        .build();
  }

  private static ReadRowsRequest createRequest(RowRange range) {
    return ReadRowsRequest.newBuilder().setRows(RowSet.newBuilder().addRowRanges(range)).build();
  }

  private static RowRange createRowRangeClosedStart(ByteString startClosed, ByteString endOpen) {
    return RowRange.newBuilder().setStartKeyClosed(startClosed).setEndKeyOpen(endOpen).build();
  }

  private static RowRange createRowRangeOpenedStart(ByteString startOpened, ByteString endOpen) {
    return RowRange.newBuilder().setStartKeyOpen(startOpened).setEndKeyOpen(endOpen).build();
  }

  private ReadRowsRequest createKeysRequest(Iterable<ByteString> keys) {
    return ReadRowsRequest.newBuilder().setRows(createRowSet(keys)).build();
  }

  private RowSet createRowSet(Iterable<ByteString> keys) {
    return RowSet.newBuilder().addAllRowKeys(keys).build();
  }


  /**
   * Test a single, full table scan scenario for {@Link ResumingStreamingResultScanner#filterRows()}
   * .
   * @throws IOException
   */
  @Test
  public void test_filterRows_testAllRange() throws IOException{
    ByteString key1 = ByteString.copyFrom("row1".getBytes());

    ReadRowsRequest originalRequest =
        createRequest(createRowRangeClosedStart(ByteString.EMPTY, ByteString.EMPTY));

    ReadRowsRequestManager underTest = new ReadRowsRequestManager(originalRequest);

    underTest.updateLastFoundKey(key1);
    Assert.assertEquals(createRequest(createRowRangeOpenedStart(key1, ByteString.EMPTY)),
      underTest.buildUpdatedRequest());
  }

  /**
   * Test rowKeys scenario for {@Link ReadRowsRequestManager#getUpdatedRequest()}.
   * @throws IOException
   */
  @Test
  public void test_filterRows_rowKeys() throws IOException{
    ByteString key1 = ByteString.copyFrom("row1".getBytes());
    ByteString key2 = ByteString.copyFrom("row2".getBytes());
    ByteString key3 = ByteString.copyFrom("row3".getBytes());

    ReadRowsRequest originalRequest = createKeysRequest(Arrays.asList(key1, key2, key3));

    ReadRowsRequestManager underTest = new ReadRowsRequestManager(originalRequest);

    Assert.assertEquals(originalRequest, underTest.buildUpdatedRequest());
    underTest.updateLastFoundKey(key1);

    Assert.assertEquals(createKeysRequest(Arrays.asList(key2, key3)),
      underTest.buildUpdatedRequest());
  }

  /**
   * Test multiple rowset filter scenarios for {@Link ReadRowsRequestManager#getUpdatedRequest()}.
   * @throws IOException
   */
  @Test
  public void test_filterRows_multiRowSetFilters() throws IOException{
    ByteString key1 = ByteString.copyFrom("row1".getBytes());
    ByteString key2 = ByteString.copyFrom("row2".getBytes());
    ByteString key3 = ByteString.copyFrom("row3".getBytes());

    RowSet fullRowSet = RowSet.newBuilder()
        .addAllRowKeys(Arrays.asList(key1, key2, key3)) // row1 should be filtered out
        .addRowRanges(RowRange.newBuilder().setStartKeyOpen(BLANK).setEndKeyClosed(key1)) // should be filtered out
        .addRowRanges(RowRange.newBuilder().setStartKeyOpen(BLANK).setEndKeyOpen(key1)) // should be filtered out
        .addRowRanges(RowRange.newBuilder().setStartKeyOpen(key1).setEndKeyOpen(key2)) // should stay
        .addRowRanges(RowRange.newBuilder().setStartKeyClosed(key1).setEndKeyOpen(key2)) // should be converted (key1 -> key2)
        .addRowRanges(RowRange.newBuilder().setStartKeyClosed(key1).setEndKeyClosed(key2)) // should be converted (key1 -> key2]
        .addRowRanges(RowRange.newBuilder().setStartKeyOpen(key2).setEndKeyOpen(key3)) // should stay
        .addRowRanges(RowRange.newBuilder().setStartKeyClosed(key2).setEndKeyOpen(key3)) // should stay
        .build();

    RowSet filteredRowSet = RowSet.newBuilder()
        .addAllRowKeys(Arrays.asList(key2, key3)) // row1 should be filtered out
        .addRowRanges(RowRange.newBuilder().setStartKeyOpen(key1).setEndKeyOpen(key2)) // should stay
        .addRowRanges(RowRange.newBuilder().setStartKeyOpen(key1).setEndKeyOpen(key2)) // should be converted (key1 -> key2)
        .addRowRanges(RowRange.newBuilder().setStartKeyOpen(key1).setEndKeyClosed(key2)) // should be converted (key1 -> key2]
        .addRowRanges(RowRange.newBuilder().setStartKeyOpen(key2).setEndKeyOpen(key3)) // should stay
        .addRowRanges(RowRange.newBuilder().setStartKeyClosed(key2).setEndKeyOpen(key3)) // should stay
        .build();

    ReadRowsRequest originalRequest = ReadRowsRequest.newBuilder().setRows(fullRowSet).build();
    ReadRowsRequest filteredRequest = ReadRowsRequest.newBuilder().setRows(filteredRowSet).build();

    ReadRowsRequestManager underTest = new ReadRowsRequestManager(originalRequest);
    Assert.assertEquals(originalRequest, underTest.buildUpdatedRequest());
    underTest.updateLastFoundKey(key1);
    Assert.assertEquals(filteredRequest, underTest.buildUpdatedRequest());
  }

  /**
   * Test that resume handles key requests as unsigned bytes
   * @throws IOException
   */
  @Test
  public void test_filterRows_unsignedRange() throws IOException {
    ByteString key1 = ByteString.copyFrom(new byte[] {0x7f});
    ByteString key2 = ByteString.copyFrom(new byte[] { (byte)0x80});

    ReadRowsRequest originalRequest =
        createRequest(createRowRangeClosedStart(key1, ByteString.EMPTY));

    ReadRowsRequestManager underTest = new ReadRowsRequestManager(originalRequest);
    underTest.updateLastFoundKey(key2);

    Assert.assertEquals(createRequest(createRowRangeOpenedStart(key2, ByteString.EMPTY)),
      underTest.buildUpdatedRequest());
  }

  /**
   * Test that resume handles row ranges as unsigned bytes
   * @throws IOException
   */
  @Test
  public void test_filterRows_unsignedRows() throws IOException {
    ByteString key1 = ByteString.copyFrom(new byte[] {0x7f});
    ByteString key2 = ByteString.copyFrom(new byte[] { (byte)0x80});

    ReadRowsRequest originalRequest = createKeysRequest(Arrays.asList(key1, key2));

    ReadRowsRequestManager underTest = new ReadRowsRequestManager(originalRequest);
    underTest.updateLastFoundKey(key1);

    Assert.assertEquals(createKeysRequest(Arrays.asList(key2)), underTest.buildUpdatedRequest());
  }
}
