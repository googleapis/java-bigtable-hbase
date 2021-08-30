/*
 * Copyright 2017 Google LLC
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

import com.google.api.core.InternalApi;
import com.google.bigtable.v2.RowRange;
import com.google.bigtable.v2.RowSet;
import com.google.cloud.bigtable.data.v2.models.Query;
import com.google.cloud.bigtable.data.v2.models.Range.ByteStringRange;
import com.google.cloud.bigtable.hbase.util.RowKeyWrapper;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.BoundType;
import com.google.common.collect.Range;
import com.google.common.collect.RangeSet;
import com.google.common.collect.TreeRangeSet;
import com.google.protobuf.ByteString;

/**
 * Adapter to convert between a Bigtable {@link RowSet} and guava's {@link RangeSet}.
 *
 * <p>For internal use only - public for technical reasons.
 */
@InternalApi("For internal usage only")
public class RowRangeAdapter {

  /**
   * Convert bigtable's {@link RowSet} -> Guava's {@link RangeSet}. Note that this will normalize
   * the ranges, such that: overlapping keys and ranges will be merged and empty range keys will be
   * converted in boundless ranges.
   */
  @VisibleForTesting
  RangeSet<RowKeyWrapper> rowSetToRangeSet(RowSet btRowSet) {
    RangeSet<RowKeyWrapper> rangeSet = TreeRangeSet.create();
    for (RowRange btRowRange : btRowSet.getRowRangesList()) {
      rangeSet.add(rowRangeToRange(btRowRange));
    }
    for (ByteString key : btRowSet.getRowKeysList()) {
      rangeSet.add(Range.singleton(new RowKeyWrapper(key)));
    }
    return rangeSet;
  }

  /** Convert Bigtable's {@link RowRange} -> guava {@link Range}. */
  private Range<RowKeyWrapper> rowRangeToRange(RowRange btRange) {
    final BoundType startBound;
    final ByteString startKey;

    switch (btRange.getStartKeyCase()) {
      case START_KEY_OPEN:
        startBound = BoundType.OPEN;
        startKey = btRange.getStartKeyOpen();
        break;
      case START_KEY_CLOSED:
        startBound = BoundType.CLOSED;
        startKey = btRange.getStartKeyClosed();
        break;
      case STARTKEY_NOT_SET:
        startBound = BoundType.CLOSED;
        startKey = ByteString.EMPTY;
        break;
      default:
        throw new IllegalArgumentException(
            "Unexpected start key case: " + btRange.getStartKeyCase());
    }

    final BoundType endBound;
    final ByteString endKey;
    switch (btRange.getEndKeyCase()) {
      case END_KEY_OPEN:
        endBound = BoundType.OPEN;
        endKey = btRange.getEndKeyOpen();
        break;
      case END_KEY_CLOSED:
        endBound = BoundType.CLOSED;
        endKey = btRange.getEndKeyClosed();
        break;
      case ENDKEY_NOT_SET:
        endBound = BoundType.OPEN;
        endKey = ByteString.EMPTY;
        break;
      default:
        throw new IllegalArgumentException("Unexpected end key case: " + btRange.getEndKeyCase());
    }

    return boundedRange(startBound, startKey, endBound, endKey);
  }

  /** To determine {@link Range<RowKeyWrapper>} based start/end key & {@link BoundType}. */
  @VisibleForTesting
  Range<RowKeyWrapper> boundedRange(
      BoundType startBound, ByteString startKey, BoundType endBound, ByteString endKey) {
    // Bigtable doesn't allow empty row keys, so an empty start row which is open or closed is
    // considered unbounded. ie. all row keys are bigger than the empty key (no need to
    // differentiate between open/closed)
    final boolean startUnbounded = startKey.isEmpty();

    // Bigtable doesn't allow empty row keys, so an empty end row which is open or closed is
    // considered unbounded. ie. all row keys are smaller than the empty end key (no need to
    // differentiate between open/closed)
    final boolean endUnbounded = endKey.isEmpty();

    if (startUnbounded && endUnbounded) {
      return Range.all();
    } else if (startUnbounded) {
      return Range.upTo(new RowKeyWrapper(endKey), endBound);
    } else if (endUnbounded) {
      return Range.downTo(new RowKeyWrapper(startKey), startBound);
    } else {
      return Range.range(
          new RowKeyWrapper(startKey), startBound, new RowKeyWrapper(endKey), endBound);
    }
  }

  /**
   * Convert guava's {@link RangeSet} to Bigtable's {@link ByteStringRange}. Please note that this
   * will convert boundless ranges into unset key cases.
   */
  @VisibleForTesting
  void rangeSetToByteStringRange(RangeSet<RowKeyWrapper> guavaRangeSet, Query query) {

    for (Range<RowKeyWrapper> guavaRange : guavaRangeSet.asRanges()) {
      // Is it a point?
      if (guavaRange.hasLowerBound()
          && guavaRange.lowerBoundType() == BoundType.CLOSED
          && guavaRange.hasUpperBound()
          && guavaRange.upperBoundType() == BoundType.CLOSED
          && guavaRange.lowerEndpoint().equals(guavaRange.upperEndpoint())) {

        query.rowKey(guavaRange.lowerEndpoint().getKey());
      } else {
        ByteStringRange byteRange = ByteStringRange.unbounded();

        // Handle start key
        if (guavaRange.hasLowerBound()) {
          switch (guavaRange.lowerBoundType()) {
            case CLOSED:
              byteRange.startClosed(guavaRange.lowerEndpoint().getKey());
              break;
            case OPEN:
              byteRange.startOpen(guavaRange.lowerEndpoint().getKey());
              break;
            default:
              throw new IllegalArgumentException(
                  "Unexpected lower bound type: " + guavaRange.lowerBoundType());
          }
        }

        // handle end key
        if (guavaRange.hasUpperBound()) {
          switch (guavaRange.upperBoundType()) {
            case CLOSED:
              byteRange.endClosed(guavaRange.upperEndpoint().getKey());
              break;
            case OPEN:
              byteRange.endOpen(guavaRange.upperEndpoint().getKey());
              break;
            default:
              throw new IllegalArgumentException(
                  "Unexpected upper bound type: " + guavaRange.upperBoundType());
          }
        }
        query.range(byteRange);
      }
    }
  }
}
