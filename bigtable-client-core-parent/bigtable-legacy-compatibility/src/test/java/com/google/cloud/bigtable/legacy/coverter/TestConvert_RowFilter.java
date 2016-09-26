/*
 * Copyright 2016 Google Inc. All Rights Reserved.
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
package com.google.cloud.bigtable.legacy.coverter;

import org.junit.Assert;
import org.junit.Test;
import com.google.bigtable.v2.TimestampRangeOrBuilder;
import com.google.protobuf.ByteString;
import com.google.protobuf.InvalidProtocolBufferException;

/**
 * Tests {@link BigtableLegacyProtobufConverter} for various types of
 * {@link com.google.bigtable.v1.RowFilter} scenarios.
 * @author sduskis
 */

public class TestConvert_RowFilter {

  @Test
  public void testRowFilter_default() throws InvalidProtocolBufferException {
    BigtableLegacyProtobufConverter.convert(com.google.bigtable.v1.RowFilter.getDefaultInstance());
  }

  /**
   * The structure of {@link com.google.bigtable.v1.ColumnRange} stayed the same across v1 and v2,
   * but the names of the elements changed. Test to ensure compatibility with v2's {@link
   * com.google.bigtable.v2.ColumnRange}.
   *
   * @throws InvalidProtocolBufferException
   */
  @Test
  public void testRowFilter_ColumnRange() throws InvalidProtocolBufferException {
    com.google.bigtable.v1.ColumnRange v1ColumnRange = com.google.bigtable.v1.ColumnRange
        .newBuilder()
        .setStartQualifierInclusive(ByteString.copyFrom("start".getBytes()))
        .setEndQualifierExclusive(ByteString.copyFrom("end".getBytes()))
        .build();
    com.google.bigtable.v1.RowFilter v1RowFilter =
        com.google.bigtable.v1.RowFilter.newBuilder().setColumnRangeFilter(v1ColumnRange).build();
    com.google.bigtable.v2.RowFilter v2RowFilter =
        BigtableLegacyProtobufConverter.convert(v1RowFilter);

    com.google.bigtable.v2.ColumnRange v2ColumnRange = v2RowFilter.getColumnRangeFilter();

    Assert.assertEquals(v1ColumnRange.getStartQualifierInclusive(),
      v2ColumnRange.getStartQualifierClosed());
    Assert.assertEquals(v1ColumnRange.getEndQualifierExclusive(),
      v2ColumnRange.getEndQualifierOpen());
  }

  /**
   * The structure of {@link com.google.bigtable.v1.TimestampRange} stayed the same across v1 and
   * v2, but the names of the elements changed. Test to ensure compatibility with v2's {@link
   * com.google.bigtable.v2.TimestampRange}.
   *
   * @throws InvalidProtocolBufferException
   */
  @Test
  public void testRowFilter_TimestampRange() throws InvalidProtocolBufferException {
    com.google.bigtable.v1.TimestampRange v1TimestampRange =
        com.google.bigtable.v1.TimestampRange.newBuilder()
            .setStartTimestampMicros(1_000_000)
            .setEndTimestampMicros(2_000_000)
            .build();
    com.google.bigtable.v1.RowFilter v1RowFilter =
        com.google.bigtable.v1.RowFilter.newBuilder()
            .setTimestampRangeFilter(v1TimestampRange)
            .build();
    com.google.bigtable.v2.RowFilter v2RowFilter =
        BigtableLegacyProtobufConverter.convert(v1RowFilter);

    com.google.bigtable.v2.TimestampRangeOrBuilder v2TimestampRange =
        v2RowFilter.getTimestampRangeFilterOrBuilder();

    Assert.assertEquals(v1TimestampRange.getStartTimestampMicros(),
      v2TimestampRange.getStartTimestampMicros());
    Assert.assertEquals(v1TimestampRange.getEndTimestampMicros(),
      v2TimestampRange.getEndTimestampMicros());
  }

  // TODO: MORE TESTS!
}
