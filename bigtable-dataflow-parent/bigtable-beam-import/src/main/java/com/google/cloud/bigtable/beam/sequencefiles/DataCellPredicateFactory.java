/*
 * Copyright 2017 Google Inc. All Rights Reserved.
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
package com.google.cloud.bigtable.beam.sequencefiles;

import static com.google.common.base.Preconditions.checkArgument;

import com.google.common.base.Function;
import com.google.common.base.Predicate;
import com.google.common.base.Predicates;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellComparator;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.KeyValue;

/**
 * Function that creates a {@link Predicate}s from a Delete Marker {@code Cell}. The
 * {@code Predicate} returns {@code true} for all cells that should be deleted because of this
 * Delete Marker.
 *
 * <p>A predicate assumes that all cells to be filtered belong to the same row and column family
 * as the seed Delete Marker, and does NOT validate this assumption on cells encountered.
 *
 * @author sduskis
 * @version $Id: $Id
 */
public class DataCellPredicateFactory implements Function<Cell, Predicate<Cell>> {

  /** {@inheritDoc} */
  @Override
  public Predicate<Cell> apply(Cell deleteMarker) {
    checkArgument(CellUtil.isDelete(deleteMarker), "expecting delete marker cell.");
    switch (KeyValue.Type.codeToType(deleteMarker.getTypeByte())) {
      case Delete:
        // Delete specific column by exact timestamp.
        return Predicates.and(
            new MatchQualifier(deleteMarker), new MatchExactTimestamp(deleteMarker));
      case DeleteFamilyVersion:
        // Delete all column in family by exact timestamp.
        return new MatchExactTimestamp(deleteMarker);
      case DeleteColumn:
        // Delete specific column up to a timestamp
        return Predicates.and(
            new MatchQualifier(deleteMarker), new MatchTimestampRangeByUpperBound(deleteMarker));
      case DeleteFamily:
        // Delete all columns in a family up to a timestamp
        return new MatchTimestampRangeByUpperBound(deleteMarker);
      default: throw new IllegalStateException(
          "Unexpected type byte: " + deleteMarker.getTypeByte());
    }
  }

  private static class MatchQualifier implements Predicate<Cell> {
    private final Cell deleteMarker;

    MatchQualifier(Cell deleteMarker) {
      this.deleteMarker = deleteMarker;
    }

    @Override
    public boolean apply(Cell cell) {
      return CellComparator.compareQualifiers(deleteMarker, cell) == 0;
    }
  }

  private static class MatchExactTimestamp implements Predicate<Cell> {
    private final Cell deleteMarker;

    MatchExactTimestamp(Cell deleteMarker) {
      this.deleteMarker = deleteMarker;
    }

    @Override
    public boolean apply(Cell cell) {
      return CellComparator.compareTimestamps(deleteMarker, cell) == 0;
    }
  }

  private static class MatchTimestampRangeByUpperBound implements Predicate<Cell> {
    private final Cell deleteMarker;

    MatchTimestampRangeByUpperBound(Cell deleteMarker) {
      this.deleteMarker = deleteMarker;
    }

    @Override
    public boolean apply(Cell cell) {
      // Returns true if deleteMarker.ts >= cell.ts. i.e. the cell should be deleted.
      // CellComparator orders timestamps high to low so '<= 0' is need here.
      return CellComparator.compareTimestamps(deleteMarker, cell) <= 0;
    }
  }
}
