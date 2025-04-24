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
package com.google.cloud.bigtable.hbase.adapters.filters;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

import com.google.cloud.bigtable.hbase.adapters.ResponseAdapter;
import com.google.cloud.bigtable.hbase.adapters.read.RowCell;
import com.google.common.collect.ImmutableList;
import java.io.IOException;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnit;
import org.mockito.junit.MockitoRule;

/** Unit tests for the {@link BigtableWhileMatchResultScannerAdapter}. */
@RunWith(JUnit4.class)
public class TestBigtableWhileMatchResultScannerAdapter {
  @Rule public final MockitoRule mockitoRule = MockitoJUnit.rule();

  @Mock private ResponseAdapter mockRowAdapter;

  @Mock ResultScanner mockBigtableResultScanner;

  private BigtableWhileMatchResultScannerAdapter adapter;

  @Before
  public void setUp() {
    adapter = new BigtableWhileMatchResultScannerAdapter();
  }

  @Test
  public void adapt_noRow() throws IOException {
    when(mockBigtableResultScanner.next()).thenReturn(null);

    ResultScanner scanner = adapter.adapt(mockBigtableResultScanner);
    assertNull(scanner.next());
    verify(mockBigtableResultScanner).next();
    verifyNoInteractions(mockRowAdapter);
  }

  @Test
  public void adapt_oneRow() throws IOException {
    Result expectedResult =
        Result.create(
            ImmutableList.<Cell>of(
                new RowCell(
                    Bytes.toBytes("row"),
                    Bytes.toBytes("family"),
                    Bytes.toBytes("q"),
                    10000L,
                    Bytes.toBytes("value"),
                    ImmutableList.<String>of())));
    when(mockBigtableResultScanner.next()).thenReturn(expectedResult);

    ResultScanner scanner = adapter.adapt(mockBigtableResultScanner);
    assertArrayEquals(expectedResult.rawCells(), scanner.next().rawCells());
    verify(mockBigtableResultScanner).next();
  }

  @Test
  public void adapt_oneRow_hasMatchingLabels() throws IOException {
    Result expectedResult =
        Result.create(
            ImmutableList.<Cell>of(
                new RowCell(
                    Bytes.toBytes("row"),
                    Bytes.toBytes("family"),
                    Bytes.toBytes("q"),
                    10000L,
                    Bytes.toBytes("value"),
                    ImmutableList.of("a-in")),
                new RowCell(
                    Bytes.toBytes("row"),
                    Bytes.toBytes("family"),
                    Bytes.toBytes("q"),
                    10000L,
                    Bytes.toBytes("value"),
                    ImmutableList.of("a-out"))));
    when(mockBigtableResultScanner.next()).thenReturn(expectedResult);

    ResultScanner scanner = adapter.adapt(mockBigtableResultScanner);
    assertEquals(0, scanner.next().size());
    verify(mockBigtableResultScanner).next();
  }

  @Test
  public void adapt_oneRow_hasNoMatchingLabels() throws IOException {
    Result expectedResult =
        Result.create(
            ImmutableList.<Cell>of(
                new RowCell(
                    Bytes.toBytes("key"),
                    Bytes.toBytes("family"),
                    Bytes.toBytes("q"),
                    10000L,
                    Bytes.toBytes("value"),
                    ImmutableList.of("a-out"))));
    when(mockBigtableResultScanner.next()).thenReturn(expectedResult);

    ResultScanner scanner = adapter.adapt(mockBigtableResultScanner);
    assertNull(scanner.next());
    verify(mockBigtableResultScanner).next();
  }
}
