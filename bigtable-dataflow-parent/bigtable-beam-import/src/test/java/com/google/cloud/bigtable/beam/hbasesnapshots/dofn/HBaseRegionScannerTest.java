/*
 * Copyright 2026 Google LLC
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
package com.google.cloud.bigtable.beam.hbasesnapshots.dofn;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;

import java.io.IOException;
import java.util.List;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.RegionInfo;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.TableDescriptor;
import org.apache.hadoop.hbase.client.TableDescriptorBuilder;
import org.apache.hadoop.hbase.regionserver.HRegion;
import org.apache.hadoop.hbase.regionserver.RegionScanner;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

/** Tests the {@link HBaseRegionScanner} functionality. */
@RunWith(JUnit4.class)
public class HBaseRegionScannerTest {

  private Configuration conf;
  private FileSystem fs;
  private Path rootDir;
  private TableDescriptor htd;
  private RegionInfo hri;
  private Scan scan;
  private HRegion region;
  private RegionScanner scanner;
  private MockedStatic<HRegion> mockedHRegion;

  @Before
  public void setUp() throws Exception {
    conf = new Configuration();
    fs = mock(FileSystem.class);
    rootDir = new Path("gs://bucket/data");
    htd = TableDescriptorBuilder.newBuilder(TableName.valueOf("my-table")).build();
    hri = mock(RegionInfo.class);
    scan = new Scan();

    region = mock(HRegion.class);
    // HRegion.getScanner returns RegionScannerImpl which is package-private.
    // We try to mock it by name, falling back to the interface if not found.
    try {
      scanner =
          (RegionScanner)
              mock(Class.forName("org.apache.hadoop.hbase.regionserver.RegionScannerImpl"));
    } catch (ClassNotFoundException e) {
      scanner = mock(RegionScanner.class);
    }

    Mockito.doReturn(scanner).when(region).getScanner(scan);

    // Mock the static newHRegion method to avoid real file system access and
    // region initialization which requires a lot of setup.
    mockedHRegion = mockStatic(HRegion.class);
    mockedHRegion
        .when(
            () ->
                HRegion.newHRegion(
                    Mockito.any(Path.class),
                    Mockito.any(),
                    Mockito.any(FileSystem.class),
                    Mockito.any(Configuration.class),
                    Mockito.any(RegionInfo.class),
                    Mockito.any(TableDescriptor.class),
                    Mockito.any()))
        .thenReturn(region);
  }

  @After
  public void tearDown() {
    mockedHRegion.close();
  }

  /**
   * Tests that {@link HBaseRegionScanner#next()} successfully reads and returns records from the
   * region.
   */
  @Test
  public void testNext() throws Exception {
    Cell cell = mock(Cell.class);

    // Mock scanner.nextRaw(values) to populate the list and return false (no more rows)
    Mockito.doAnswer(
            invocation -> {
              List<Cell> list = invocation.getArgument(0);
              list.add(cell);
              return false;
            })
        .when(scanner)
        .nextRaw(Mockito.anyList());

    HBaseRegionScanner hbaseScanner = new HBaseRegionScanner(conf, fs, rootDir, htd, hri, scan);

    Result result = hbaseScanner.next();
    assertNotNull(result);
    assertFalse(result.isEmpty());

    Result resultNull = hbaseScanner.next();
    assertNull(resultNull);
  }

  /**
   * Tests that {@link HBaseRegionScanner#next()} skips empty results and continues scanning if the
   * underlying scanner has more rows.
   */
  @Test
  public void testNext_SkipsEmptyResults() throws Exception {
    Cell cell = mock(Cell.class);

    Mockito.doAnswer(
            new org.mockito.stubbing.Answer<Boolean>() {
              private int count = 0;

              @Override
              public Boolean answer(org.mockito.invocation.InvocationOnMock invocation) {
                List<Cell> list = invocation.getArgument(0);
                if (count == 0) {
                  count++;
                  return true; // hasMore = true, but empty list
                } else {
                  list.add(cell);
                  return false; // hasMore = false, with data
                }
              }
            })
        .when(scanner)
        .nextRaw(Mockito.anyList());

    HBaseRegionScanner hbaseScanner = new HBaseRegionScanner(conf, fs, rootDir, htd, hri, scan);

    Result result = hbaseScanner.next();
    assertNotNull(result);
    assertFalse(result.isEmpty());
  }

  /**
   * Tests that {@link HBaseRegionScanner#close()} releases all resources by closing the scanner and
   * region.
   */
  @Test
  public void testClose() throws Exception {
    HBaseRegionScanner hbaseScanner = new HBaseRegionScanner(conf, fs, rootDir, htd, hri, scan);

    hbaseScanner.close();

    Mockito.verify(scanner, Mockito.times(1)).close();
    Mockito.verify(region, Mockito.times(1)).closeRegionOperation();
    Mockito.verify(region, Mockito.times(1)).close(true);
  }

  @Test
  public void testConstructor_InitializationFailure() throws Exception {
    Mockito.doThrow(new IOException("Initialization failed")).when(region).initialize();

    try {
      new HBaseRegionScanner(conf, fs, rootDir, htd, hri, scan);
      org.junit.Assert.fail("Expected IOException");
    } catch (IOException e) {
      org.junit.Assert.assertEquals("Initialization failed", e.getMessage());
    }

    Mockito.verify(region, Mockito.times(1)).close(true);
    Mockito.verify(region, Mockito.never()).closeRegionOperation();
  }
}
