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
package com.google.cloud.bigtable.dataflowimport;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import com.google.cloud.bigtable.dataflowimport.testing.HBaseCellUtils;
import com.google.common.base.Predicate;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;

import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

import java.util.List;
import java.util.Set;

/**
 * Unit tests for {@link DataCellPredicateFactory}.
 */
@RunWith(JUnit4.class)
public class DataCellPredicateFactoryTest {
  private static final byte[] ROW_KEY = Bytes.toBytes("row_key");
  private static final byte[] COLUMN_FAMILY = Bytes.toBytes("column_family");
  private static final byte[] QUALIFIER = Bytes.toBytes("qualifier");
  private static final byte[] ANOTHER_QUALIFIER = Bytes.toBytes("another_qualifier");

  private DataCellPredicateFactory predicateMaker;

  @Before
  public void setup() {
    predicateMaker = new DataCellPredicateFactory();
  }

  @Test
  public void testDeleteMarkerForOneCellWithExactTimestamp() {
    long timestamp = 10000L;
    List<Cell> cells = Lists.newArrayList(
        HBaseCellUtils.createDataCell(ROW_KEY, COLUMN_FAMILY, QUALIFIER, timestamp),
        HBaseCellUtils.createDataCell(ROW_KEY, COLUMN_FAMILY, QUALIFIER, timestamp - 1),
        HBaseCellUtils.createDataCell(ROW_KEY, COLUMN_FAMILY, QUALIFIER, timestamp + 1));
    // A predicate that matches the first cell in the list.
    Predicate<Cell> predicate = predicateMaker.apply(
        HBaseCellUtils.deleteMarkerForOneCellWithExactTimestamp(
            ROW_KEY, COLUMN_FAMILY, QUALIFIER, timestamp));
    assertEquals(cells.get(0), Iterables.getOnlyElement(Iterables.filter(cells, predicate)));
    // Change qualifier and there should be no match.
    predicate = predicateMaker.apply(
        HBaseCellUtils.deleteMarkerForOneCellWithExactTimestamp(
            ROW_KEY, COLUMN_FAMILY, ANOTHER_QUALIFIER, timestamp));
    assertTrue(Iterables.isEmpty(Iterables.filter(cells,  predicate)));
  }

  @Test
  public void testDeleteMarkerForCellsWithLowerOrEqualTimestamp() {
    long timestamp = 10000L;
    List<Cell> cells = Lists.newArrayList(
        HBaseCellUtils.createDataCell(ROW_KEY, COLUMN_FAMILY, QUALIFIER, timestamp),
        HBaseCellUtils.createDataCell(ROW_KEY, COLUMN_FAMILY, QUALIFIER, timestamp - 1),
        HBaseCellUtils.createDataCell(ROW_KEY, COLUMN_FAMILY, QUALIFIER, timestamp + 1));
    // A predicate that matches the first cell in the list.
    Predicate<Cell> predicate = predicateMaker.apply(
        HBaseCellUtils.deleteMarkerForCellsWithLowerOrEqualTimestamp(
            ROW_KEY, COLUMN_FAMILY, QUALIFIER, timestamp));
    assertEquals(Sets.newHashSet(cells.get(0), cells.get(1)),
        Sets.newHashSet(Iterables.filter(cells, predicate)));
    // Change qualifier and there should be no match.
    predicate = predicateMaker.apply(
        HBaseCellUtils.deleteMarkerForCellsWithLowerOrEqualTimestamp(
            ROW_KEY, COLUMN_FAMILY, ANOTHER_QUALIFIER, timestamp));
    assertTrue(Iterables.isEmpty(Iterables.filter(cells,  predicate)));
  }

  @Test
  public void testDeleteMarkerForAllCellsInFamilyWithExactTimestamp() {
    long timestamp = 10000L;
    Predicate<Cell> predicate = predicateMaker.apply(
        HBaseCellUtils.deleteMarkerForAllCellsInFamilyWithExactTimestamp(
            ROW_KEY, COLUMN_FAMILY, timestamp));
    List<Cell> allCells = Lists.newArrayList(
        HBaseCellUtils.createDataCell(ROW_KEY, COLUMN_FAMILY, QUALIFIER, timestamp),
        HBaseCellUtils.createDataCell(ROW_KEY, COLUMN_FAMILY, ANOTHER_QUALIFIER, timestamp),
        HBaseCellUtils.createDataCell(ROW_KEY, COLUMN_FAMILY, QUALIFIER, timestamp - 1),
        HBaseCellUtils.createDataCell(ROW_KEY, COLUMN_FAMILY, ANOTHER_QUALIFIER, timestamp - 1),
        HBaseCellUtils.createDataCell(ROW_KEY, COLUMN_FAMILY, QUALIFIER, timestamp + 1),
        HBaseCellUtils.createDataCell(ROW_KEY, COLUMN_FAMILY, ANOTHER_QUALIFIER, timestamp + 1));
    Set<Cell> expected = Sets.newHashSet(allCells.get(0), allCells.get(1));
    assertEquals(expected, Sets.newHashSet(Iterables.filter(allCells, predicate)));
  }

  @Test
  public void testDeleteMarkerForAllCellsInFamilyWithLowerOrEqualTimestamp() {
    long timestamp = 10000L;
    Predicate<Cell> predicate = predicateMaker.apply(
        HBaseCellUtils.deleteMarkerForAllCellsInFamilyWithLowerOrEqualTimestamp(
            ROW_KEY, COLUMN_FAMILY, timestamp));
    List<Cell> allCells = Lists.newArrayList(
        HBaseCellUtils.createDataCell(ROW_KEY, COLUMN_FAMILY, QUALIFIER, timestamp),
        HBaseCellUtils.createDataCell(ROW_KEY, COLUMN_FAMILY, ANOTHER_QUALIFIER, timestamp),
        HBaseCellUtils.createDataCell(ROW_KEY, COLUMN_FAMILY, QUALIFIER, timestamp - 1),
        HBaseCellUtils.createDataCell(ROW_KEY, COLUMN_FAMILY, ANOTHER_QUALIFIER, timestamp - 1),
        HBaseCellUtils.createDataCell(ROW_KEY, COLUMN_FAMILY, QUALIFIER, timestamp + 1),
        HBaseCellUtils.createDataCell(ROW_KEY, COLUMN_FAMILY, ANOTHER_QUALIFIER, timestamp + 1));
    Set<Cell> expected = Sets.newHashSet(allCells.subList(0, 4));
    assertEquals(expected, Sets.newHashSet(Iterables.filter(allCells, predicate)));
  }
}
