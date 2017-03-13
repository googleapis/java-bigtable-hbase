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

import static org.junit.Assert.assertEquals;

import com.google.bigtable.v2.RowRange;
import com.google.bigtable.v2.RowSet;
import com.google.cloud.bigtable.util.RowKeyWrapper;
import com.google.common.collect.ImmutableRangeSet;
import com.google.common.collect.Range;
import com.google.common.collect.RangeSet;
import com.google.protobuf.ByteString;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
public class TestRowRangeAdapter {

  private RowRangeAdapter adapter;

  @Before
  public void setup() {
    this.adapter = new RowRangeAdapter();
  }

  @Test
  public void testEmptyRowSet() {
    RowSet in = RowSet.newBuilder().build();
    RangeSet<RowKeyWrapper> out = adapter.rowSetToRangeSet(in);

    assertEquals(ImmutableRangeSet.builder().build(), out);
    assertEquals(in, adapter.rangeSetToRowSet(out));
  }

  @Test
  public void testSingleKeyRowSet() {
    ByteString key = ByteString.copyFromUtf8("myKey");
    RowSet in = RowSet.newBuilder()
        .addRowKeys(key)
        .build();
    RangeSet<RowKeyWrapper> out = adapter.rowSetToRangeSet(in);

    RangeSet<RowKeyWrapper> expected = ImmutableRangeSet.<RowKeyWrapper>builder()
        .add(Range.closed(new RowKeyWrapper(key), new RowKeyWrapper(key)))
        .build();

    assertEquals(expected, out);
    assertEquals(in, adapter.rangeSetToRowSet(out));
  }

  @Test
  public void testSingleClosedRangeRowSet() {
    ByteString key1 = ByteString.copyFromUtf8("myKey");
    ByteString key2 = ByteString.copyFromUtf8("otherKey");

    RowSet in = RowSet.newBuilder()
        .addRowRanges(
            RowRange.newBuilder()
                .setStartKeyClosed(key1)
                .setEndKeyClosed(key2)
        )
        .build();
    RangeSet<RowKeyWrapper> out = adapter.rowSetToRangeSet(in);

    RangeSet<RowKeyWrapper> expected = ImmutableRangeSet.<RowKeyWrapper>builder()
        .add(Range.closed(new RowKeyWrapper(key1), new RowKeyWrapper(key2)))
        .build();

    assertEquals(expected, out);
    assertEquals(in, adapter.rangeSetToRowSet(out));
  }

  @Test
  public void testSingleOpenRangeRowSet() {
    ByteString key1 = ByteString.copyFromUtf8("myKey");
    ByteString key2 = ByteString.copyFromUtf8("otherKey");

    RowSet in = RowSet.newBuilder()
        .addRowRanges(
            RowRange.newBuilder()
                .setStartKeyOpen(key1)
                .setEndKeyOpen(key2)
        )
        .build();
    RangeSet<RowKeyWrapper> out = adapter.rowSetToRangeSet(in);

    RangeSet<RowKeyWrapper> expected = ImmutableRangeSet.<RowKeyWrapper>builder()
        .add(Range.open(new RowKeyWrapper(key1), new RowKeyWrapper(key2)))
        .build();

    assertEquals(expected, out);
    assertEquals(in, adapter.rangeSetToRowSet(out));
  }

  @Test
  public void testSingleOpenClosedRangeRowSet() {
    ByteString key1 = ByteString.copyFromUtf8("myKey");
    ByteString key2 = ByteString.copyFromUtf8("otherKey");

    RowSet in = RowSet.newBuilder()
        .addRowRanges(
            RowRange.newBuilder()
                .setStartKeyOpen(key1)
                .setEndKeyClosed(key2)
        )
        .build();
    RangeSet<RowKeyWrapper> out = adapter.rowSetToRangeSet(in);

    RangeSet<RowKeyWrapper> expected = ImmutableRangeSet.<RowKeyWrapper>builder()
        .add(Range.openClosed(new RowKeyWrapper(key1), new RowKeyWrapper(key2)))
        .build();

    assertEquals(expected, out);
    assertEquals(in, adapter.rangeSetToRowSet(out));
  }

  @Test
  public void testSingleClosedOpenRangeRowSet() {
    ByteString key1 = ByteString.copyFromUtf8("myKey");
    ByteString key2 = ByteString.copyFromUtf8("otherKey");

    RowSet in = RowSet.newBuilder()
        .addRowRanges(
            RowRange.newBuilder()
                .setStartKeyClosed(key1)
                .setEndKeyOpen(key2)
        )
        .build();
    RangeSet<RowKeyWrapper> out = adapter.rowSetToRangeSet(in);

    RangeSet<RowKeyWrapper> expected = ImmutableRangeSet.<RowKeyWrapper>builder()
        .add(Range.closedOpen(new RowKeyWrapper(key1), new RowKeyWrapper(key2)))
        .build();

    assertEquals(expected, out);
    assertEquals(in, adapter.rangeSetToRowSet(out));
  }

  @Test
  public void testAllRowSet() {
    RowSet in = RowSet.newBuilder()
        .addRowRanges(RowRange.newBuilder())
        .build();

    RangeSet<RowKeyWrapper> out = adapter.rowSetToRangeSet(in);

    RangeSet<RowKeyWrapper> expected = ImmutableRangeSet.<RowKeyWrapper>builder()
        .add(Range.<RowKeyWrapper>all())
        .build();

    assertEquals(expected, out);
    assertEquals(in, adapter.rangeSetToRowSet(out));
  }

  @Test
  public void testAllRowSet2() {
    RowSet in = RowSet.newBuilder()
        .addRowRanges(
            RowRange.newBuilder()
                .setStartKeyClosed(ByteString.EMPTY)
                .setEndKeyOpen(ByteString.EMPTY)
        )
        .build();

    RangeSet<RowKeyWrapper> out = adapter.rowSetToRangeSet(in);

    RangeSet<RowKeyWrapper> expected = ImmutableRangeSet.<RowKeyWrapper>builder()
        .add(Range.<RowKeyWrapper>all())
        .build();

    assertEquals(expected, out);
    // NOTE: this isn't symmetrical, ['','') is converted to its canonical form of unset bounds
  }

  @Test
  public void testGreaterThan() {
    ByteString key = ByteString.copyFromUtf8("hi");

    RowSet in = RowSet.newBuilder()
        .addRowRanges(
            RowRange.newBuilder()
                .setStartKeyOpen(key)
        )
        .build();

    RangeSet<RowKeyWrapper> out = adapter.rowSetToRangeSet(in);

    RangeSet<RowKeyWrapper> expected = ImmutableRangeSet.<RowKeyWrapper>builder()
        .add(Range.greaterThan(new RowKeyWrapper(key)))
        .build();

    assertEquals(expected, out);
    assertEquals(in, adapter.rangeSetToRowSet(out));
  }

  @Test
  public void testGreaterOrEqThan() {
    ByteString key = ByteString.copyFromUtf8("hi");

    RowSet in = RowSet.newBuilder()
        .addRowRanges(
            RowRange.newBuilder()
                .setStartKeyClosed(key)
        )
        .build();

    RangeSet<RowKeyWrapper> out = adapter.rowSetToRangeSet(in);

    RangeSet<RowKeyWrapper> expected = ImmutableRangeSet.<RowKeyWrapper>builder()
        .add(Range.atLeast(new RowKeyWrapper(key)))
        .build();

    assertEquals(expected, out);
    assertEquals(in, adapter.rangeSetToRowSet(out));
  }

  @Test
  public void testLesserThan() {
    ByteString key = ByteString.copyFromUtf8("hi");

    RowSet in = RowSet.newBuilder()
        .addRowRanges(
            RowRange.newBuilder()
                .setEndKeyOpen(key)
        )
        .build();

    RangeSet<RowKeyWrapper> out = adapter.rowSetToRangeSet(in);

    RangeSet<RowKeyWrapper> expected = ImmutableRangeSet.<RowKeyWrapper>builder()
        .add(Range.lessThan(new RowKeyWrapper(key)))
        .build();

    assertEquals(expected, out);
    assertEquals(in, adapter.rangeSetToRowSet(out));
  }

  @Test
  public void testLesserOrEqThan() {
    ByteString key = ByteString.copyFromUtf8("hi");

    RowSet in = RowSet.newBuilder()
        .addRowRanges(
            RowRange.newBuilder()
                .setEndKeyClosed(key)
        )
        .build();

    RangeSet<RowKeyWrapper> out = adapter.rowSetToRangeSet(in);

    RangeSet<RowKeyWrapper> expected = ImmutableRangeSet.<RowKeyWrapper>builder()
        .add(Range.atMost(new RowKeyWrapper(key)))
        .build();

    assertEquals(expected, out);
    assertEquals(in, adapter.rangeSetToRowSet(out));
  }

  @Test
  public void testMultipleDisjoint() {
    RowSet in = RowSet.newBuilder()
        .addRowKeys(ByteString.copyFromUtf8("point1"))
        .addRowKeys(ByteString.copyFromUtf8("point2"))
        .addRowRanges(
            RowRange.newBuilder()
                .setEndKeyClosed(ByteString.copyFromUtf8("b"))
        )
        .addRowRanges(
            RowRange.newBuilder()
                .setStartKeyClosed(ByteString.copyFromUtf8("c"))
                .setEndKeyClosed(ByteString.copyFromUtf8("d"))
        )
        .addRowRanges(
            RowRange.newBuilder()
              .setStartKeyOpen(ByteString.copyFromUtf8("y"))
        )
        .build();

    RangeSet<RowKeyWrapper> out = adapter.rowSetToRangeSet(in);

    RangeSet<RowKeyWrapper> expected = ImmutableRangeSet.<RowKeyWrapper>builder()
        .add(Range.singleton(new RowKeyWrapper(ByteString.copyFromUtf8("point1"))))
        .add(Range.singleton(new RowKeyWrapper(ByteString.copyFromUtf8("point2"))))
        .add(Range.atMost(new RowKeyWrapper(ByteString.copyFromUtf8("b"))))
        .add(
            Range.closed(
                new RowKeyWrapper(ByteString.copyFromUtf8("c")),
                new RowKeyWrapper(ByteString.copyFromUtf8("d"))
            )
        )
        .add(Range.greaterThan(new RowKeyWrapper(ByteString.copyFromUtf8("y"))))
        .build();

    assertEquals(expected, out);
    assertEquals(in, adapter.rangeSetToRowSet(out));
  }
}
