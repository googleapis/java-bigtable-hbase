/*
 * Copyright 2019 Google LLC. All Rights Reserved.
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
package com.google.cloud.bigtable.hbase.adapters.read;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

import com.google.cloud.bigtable.data.v2.models.Row;
import com.google.cloud.bigtable.data.v2.models.RowCell;
import com.google.cloud.bigtable.hbase.util.ByteStringer;
import com.google.common.collect.ImmutableList;
import com.google.protobuf.ByteString;
import java.util.Collections;
import java.util.List;
import org.apache.hadoop.hbase.client.Result;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/**
 * Unit tests for the {@link ModelRowAdapter}.
 */
@RunWith(JUnit4.class)
public class TestModelRowAdapter {

  private static final String FAMILY_1 = "firstFamily";
  private static final String FAMILY_2 = "secondFamily";
  private static final ByteString QUALIFIER_1 = ByteString.copyFromUtf8("qualifier1");
  private static final ByteString QUALIFIER_2 = ByteString.copyFromUtf8("qualifier2");
  private static final long TIMESTAMP_MS_1 = 12345600;
  private static final long TIMESTAMP_MS_2 = 65432100;
  private static final long TIMESTAMP_MILLS_1 = TIMESTAMP_MS_1/1000;
  private static final long TIMESTAMP_MILLS_2 = TIMESTAMP_MS_2/1000;
  private static final String LABEL = "label";
  private static final List<String> LABEL_LIST = Collections.emptyList();
  private static final ByteString VALUE_1 = ByteString.copyFromUtf8("test-value-1");
  private static final ByteString VALUE_2 = ByteString.copyFromUtf8("test-value-2");
  private static final ByteString VALUE_3 = ByteString.copyFromUtf8("test-value-3");
  private static final ByteString VALUE_4 = ByteString.copyFromUtf8("test-value-4");
  private static final ByteString ROW_KEY = ByteString.copyFromUtf8("test-key");

  private ModelRowAdapter adapter = new ModelRowAdapter();

  @Test
  public void testAdaptResponseWhenNull(){
    Result result = adapter.adaptResponse(null);
    assertNull(result.rawCells());
  }

  @Test
  public void testAdaptResWithEmptyRow(){
    Row row = Row.create(ROW_KEY, Collections.<RowCell>emptyList());
    Result result = adapter.adaptResponse(row);
    assertEquals(0, result.rawCells().length);
  }

  @Test
  public void testAdaptResWithRow(){
    ImmutableList.Builder<RowCell> rowCells = ImmutableList.builder();
    rowCells.add(RowCell.create(FAMILY_1, QUALIFIER_1, TIMESTAMP_MS_1, LABEL_LIST, VALUE_1));
    rowCells.add(RowCell.create(FAMILY_1, QUALIFIER_2, TIMESTAMP_MS_2, LABEL_LIST, VALUE_2));
    rowCells.add(RowCell.create(FAMILY_2, QUALIFIER_2, TIMESTAMP_MS_2, LABEL_LIST, VALUE_3));
    rowCells.add(RowCell.create(FAMILY_2, QUALIFIER_1, TIMESTAMP_MS_1, LABEL_LIST, VALUE_4));
    //Added duplicate row
    rowCells.add(RowCell.create(FAMILY_2, QUALIFIER_1, TIMESTAMP_MS_1, LABEL_LIST, VALUE_4));
    //Added Row with label
    rowCells.add(RowCell.create(FAMILY_1, QUALIFIER_2, TIMESTAMP_MILLS_1,
        Collections.singletonList(LABEL), VALUE_1));
    Row inputRow = Row.create(ROW_KEY, rowCells.build());
    Result result = adapter.adaptResponse(inputRow);
    assertEquals(4, result.rawCells().length);

    // The duplicate row and label cells have been removed. The timestamp micros get converted to
    // millisecond accuracy.
    byte[] keyArray = ByteStringer.extract(ROW_KEY);
    org.apache.hadoop.hbase.Cell[] expectedCells = new org.apache.hadoop.hbase.Cell[] {
        new com.google.cloud.bigtable.hbase.adapters.read.RowCell(keyArray, FAMILY_1.getBytes(),
            QUALIFIER_1.toByteArray(), TIMESTAMP_MILLS_1, VALUE_1.toByteArray()),
        new com.google.cloud.bigtable.hbase.adapters.read.RowCell(keyArray, FAMILY_1.getBytes(),
            QUALIFIER_2.toByteArray(), TIMESTAMP_MILLS_2, VALUE_2.toByteArray()),
        new com.google.cloud.bigtable.hbase.adapters.read.RowCell(keyArray, FAMILY_2.getBytes(),
            QUALIFIER_1.toByteArray(), TIMESTAMP_MILLS_1, VALUE_3.toByteArray()),
        new com.google.cloud.bigtable.hbase.adapters.read.RowCell(keyArray, FAMILY_2.getBytes(),
            QUALIFIER_2.toByteArray(), TIMESTAMP_MILLS_2, VALUE_4.toByteArray()),
    };
    assertArrayEquals(expectedCells, result.rawCells());
  }
}
