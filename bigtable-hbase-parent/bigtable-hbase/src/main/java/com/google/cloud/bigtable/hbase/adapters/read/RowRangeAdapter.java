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
package com.google.cloud.bigtable.hbase.adapters.read;

import com.google.bigtable.v2.RowRange;
import com.google.bigtable.v2.RowSet;
import com.google.cloud.bigtable.util.RowKeyWrapper;
import com.google.common.collect.BoundType;
import com.google.common.collect.Range;
import com.google.common.collect.RangeSet;
import com.google.common.collect.TreeRangeSet;
import com.google.protobuf.ByteString;

/**
 * Adapter to convert between a Bigtable RowSet and guava's RangeSet.
 */
public class RowRangeAdapter {

  /**
   * Convert bigtable's RowSet -> Guava's RangeSet.
   * Note that this will normalize the ranges, such that: overlapping keys and ranges will be merged
   * and empty range keys will be converted in boundless ranges.
   */
  RangeSet<RowKeyWrapper> rowSetToRangeSet(RowSet rowSet) {
    RangeSet<RowKeyWrapper> rangeSet = TreeRangeSet.create();
    for (RowRange rowRange : rowSet.getRowRangesList()) {
      rangeSet.add(rowRangeToRange(rowRange));
    }
    for (ByteString key : rowSet.getRowKeysList()) {
      rangeSet.add(Range.singleton(new RowKeyWrapper(key)));
    }
    return rangeSet;
  }

  /**
   * Convert Bigtable's RowRange -> guava Range.
   */
  private Range<RowKeyWrapper> rowRangeToRange(RowRange range) {
    final BoundType startBound;
    final ByteString startKey;

    switch (range.getStartKeyCase()) {
      case START_KEY_OPEN:
        startBound = BoundType.OPEN;
        startKey = range.getStartKeyOpen();
        break;
      case START_KEY_CLOSED:
        startBound = BoundType.CLOSED;
        startKey = range.getStartKeyClosed();
        break;
      case STARTKEY_NOT_SET:
        startBound = BoundType.CLOSED;
        startKey = ByteString.EMPTY;
        break;
      default:
        throw new IllegalArgumentException("Unexpected start key case: " + range.getStartKeyCase());
    }
    // Bigtable doesn't allow empty row keys, so an empty start row which is open or closed is considered unbounded
    // ie. all row keys are bigger than the empty key (no need to differentiate between open/closed)
    final boolean startUnbounded = startKey.isEmpty();

    final BoundType endBound;
    final ByteString endKey;
    switch (range.getEndKeyCase()) {
      case END_KEY_OPEN:
        endBound = BoundType.OPEN;
        endKey = range.getEndKeyOpen();
        break;
      case END_KEY_CLOSED:
        endBound = BoundType.CLOSED;
        endKey = range.getEndKeyClosed();
        break;
      case ENDKEY_NOT_SET:
        endBound = BoundType.OPEN;
        endKey = ByteString.EMPTY;
        break;
      default:
        throw new IllegalArgumentException("Unexpected end key case: " + range.getEndKeyCase());
    }
    // Bigtable doesn't allow empty row keys, so an empty end row which is open or closed is considered unbounded
    // ie. all row keys are smaller than the empty end key (no need to differentiate between open/closed)
    final boolean endUnbounded = endKey.isEmpty();

    if (startUnbounded && endUnbounded) {
      return Range.all();
    } else if (startUnbounded) {
      return Range.upTo(new RowKeyWrapper(endKey), endBound);
    } else if (endUnbounded) {
      return Range.downTo(new RowKeyWrapper(startKey), startBound);
    } else {
      return Range.range(new RowKeyWrapper(startKey), startBound, new RowKeyWrapper(endKey), endBound);
    }
  }

  /**
   * Convert guava's RangeSet to Bigtable's RowSet. Please note that this will convert
   * boundless ranges into unset key cases.
   */
  RowSet rangeSetToRowSet(RangeSet<RowKeyWrapper> rangeSet) {
    RowSet.Builder rowSet = RowSet.newBuilder();

    for (Range<RowKeyWrapper> range1 : rangeSet.asRanges()) {
      // Is it a point?
      if (range1.hasLowerBound() && range1.lowerBoundType() == BoundType.CLOSED
          && range1.hasUpperBound() && range1.upperBoundType() == BoundType.CLOSED
          && range1.lowerEndpoint().equals(range1.upperEndpoint())) {

        rowSet.addRowKeys(range1.lowerEndpoint().getKey());
      } else {
        RowRange.Builder range2 = RowRange.newBuilder();

        // Handle start key
        if (range1.hasLowerBound()) {
          switch (range1.lowerBoundType()) {
            case CLOSED:
              range2.setStartKeyClosed(range1.lowerEndpoint().getKey());
              break;
            case OPEN:
              range2.setStartKeyOpen(range1.lowerEndpoint().getKey());
              break;
            default:
              throw new IllegalArgumentException("Unexpected lower bound type: " + range1.lowerBoundType());
          }
        }

        // handle end key
        if (range1.hasUpperBound()) {
          switch (range1.upperBoundType()) {
            case CLOSED:
              range2.setEndKeyClosed(range1.upperEndpoint().getKey());
              break;
            case OPEN:
              range2.setEndKeyOpen(range1.upperEndpoint().getKey());
              break;
            default:
              throw new IllegalArgumentException("Unexpected upper bound type: " + range1.upperBoundType());
          }
        }
        rowSet.addRowRanges(range2);
      }
    }
    return rowSet.build();
  }
}
