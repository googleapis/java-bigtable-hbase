/*
 * Copyright 2018 Google LLC
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.google.cloud.bigtable.data.v2.wrappers;

import com.google.cloud.bigtable.data.v2.wrappers.Range.BoundType;
import com.google.cloud.bigtable.data.v2.wrappers.Range.ByteStringRange;
import com.google.cloud.bigtable.data.v2.wrappers.Range.TimestampRange;
import com.google.protobuf.ByteString;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
public class RangeTest {
  @Test
  public void timestampUnboundedTest() {
    TimestampRange range = TimestampRange.unbounded();
    Assert.assertEquals(BoundType.UNBOUNDED, range.getStartBound());
    Assert.assertEquals(BoundType.UNBOUNDED, range.getEndBound());

    Throwable actualError = null;
    try {
      //noinspection ResultOfMethodCallIgnored
      range.getStart();
    } catch (Throwable e) {
      actualError = e;
    }
    Assert.assertTrue(actualError instanceof IllegalStateException);

    try {
      //noinspection ResultOfMethodCallIgnored
      range.getEnd();
    } catch (Throwable e) {
      actualError = e;
    }
    Assert.assertTrue(actualError instanceof IllegalStateException);
  }

  @Test
  public void timestampOfTest() {
    TimestampRange range = TimestampRange.create(10, 2_000);
    Assert.assertEquals(BoundType.CLOSED, range.getStartBound());
    Assert.assertEquals(10, range.getStart().longValue());
    Assert.assertEquals(BoundType.OPEN, range.getEndBound());
    Assert.assertEquals(2_000, range.getEnd().longValue());
  }

  @Test
  public void timestampChangeStartTest() {
    TimestampRange range = TimestampRange.create(10, 2_000).startOpen(20L);

    Assert.assertEquals(BoundType.OPEN, range.getEndBound());
    Assert.assertEquals(2_000, range.getEnd().longValue());

    Assert.assertEquals(BoundType.OPEN, range.getStartBound());
    Assert.assertEquals(20, range.getStart().longValue());

    range = range.startClosed(30L);
    Assert.assertEquals(BoundType.CLOSED, range.getStartBound());
    Assert.assertEquals(30, range.getStart().longValue());
  }

  @Test
  public void timestampChangeEndTest() {
    TimestampRange range = TimestampRange.create(10, 2_000).endClosed(1_000L);

    Assert.assertEquals(BoundType.CLOSED, range.getStartBound());
    Assert.assertEquals(10, range.getStart().longValue());

    Assert.assertEquals(BoundType.CLOSED, range.getEndBound());
    Assert.assertEquals(1_000, range.getEnd().longValue());

    range = range.endOpen(3_000L);
    Assert.assertEquals(BoundType.OPEN, range.getEndBound());
    Assert.assertEquals(3_000, range.getEnd().longValue());
  }

  @Test
  public void byteStringUnboundedTest() {
    ByteStringRange range = ByteStringRange.unbounded();
    Assert.assertEquals(BoundType.UNBOUNDED, range.getStartBound());
    Assert.assertEquals(BoundType.UNBOUNDED, range.getEndBound());

    Throwable actualError = null;
    try {
      range.getStart();
    } catch (Throwable e) {
      actualError = e;
    }
    Assert.assertTrue(actualError instanceof IllegalStateException);

    try {
      range.getEnd();
    } catch (Throwable e) {
      actualError = e;
    }
    Assert.assertTrue(actualError instanceof IllegalStateException);
  }

  @Test
  public void byteStringOfTest() {
    ByteStringRange range =
        ByteStringRange.create(ByteString.copyFromUtf8("a"), ByteString.copyFromUtf8("b"));

    Assert.assertEquals(BoundType.CLOSED, range.getStartBound());
    Assert.assertEquals(ByteString.copyFromUtf8("a"), range.getStart());
    Assert.assertEquals(BoundType.OPEN, range.getEndBound());
    Assert.assertEquals(ByteString.copyFromUtf8("b"), range.getEnd());
  }

  @Test
  public void byteStringOfStringTest() {
    ByteStringRange range = ByteStringRange.create("a", "b");

    Assert.assertEquals(BoundType.CLOSED, range.getStartBound());
    Assert.assertEquals(ByteString.copyFromUtf8("a"), range.getStart());
    Assert.assertEquals(BoundType.OPEN, range.getEndBound());
    Assert.assertEquals(ByteString.copyFromUtf8("b"), range.getEnd());
  }

  @Test
  public void byteStringChangeStartTest() {
    ByteStringRange range =
        ByteStringRange.create(ByteString.copyFromUtf8("a"), ByteString.copyFromUtf8("z"))
            .startOpen(ByteString.copyFromUtf8("b"));

    Assert.assertEquals(BoundType.OPEN, range.getEndBound());
    Assert.assertEquals(ByteString.copyFromUtf8("z"), range.getEnd());

    Assert.assertEquals(BoundType.OPEN, range.getStartBound());
    Assert.assertEquals(ByteString.copyFromUtf8("b"), range.getStart());

    range = range.startClosed(ByteString.copyFromUtf8("c"));
    Assert.assertEquals(BoundType.CLOSED, range.getStartBound());
    Assert.assertEquals(ByteString.copyFromUtf8("c"), range.getStart());
  }

  @Test
  public void byteStringChangeStartStringTest() {
    ByteStringRange range = ByteStringRange.create("a", "z").startOpen("b");

    Assert.assertEquals(BoundType.OPEN, range.getEndBound());
    Assert.assertEquals(ByteString.copyFromUtf8("z"), range.getEnd());

    Assert.assertEquals(BoundType.OPEN, range.getStartBound());
    Assert.assertEquals(ByteString.copyFromUtf8("b"), range.getStart());

    range = range.startClosed("c");
    Assert.assertEquals(BoundType.CLOSED, range.getStartBound());
    Assert.assertEquals(ByteString.copyFromUtf8("c"), range.getStart());
  }

  @Test
  public void byteStringChangeEndTest() {
    ByteStringRange range =
        ByteStringRange.create(ByteString.copyFromUtf8("a"), ByteString.copyFromUtf8("z"))
            .endClosed(ByteString.copyFromUtf8("y"));

    Assert.assertEquals(BoundType.CLOSED, range.getStartBound());
    Assert.assertEquals(ByteString.copyFromUtf8("a"), range.getStart());

    Assert.assertEquals(BoundType.CLOSED, range.getEndBound());
    Assert.assertEquals(ByteString.copyFromUtf8("y"), range.getEnd());

    range = range.endOpen(ByteString.copyFromUtf8("x"));
    Assert.assertEquals(BoundType.OPEN, range.getEndBound());
    Assert.assertEquals(ByteString.copyFromUtf8("x"), range.getEnd());
  }

  @Test
  public void byteStringChangeEndStringTest() {
    ByteStringRange range = ByteStringRange.create("a", "z").endClosed("y");

    Assert.assertEquals(BoundType.CLOSED, range.getStartBound());
    Assert.assertEquals(ByteString.copyFromUtf8("a"), range.getStart());

    Assert.assertEquals(BoundType.CLOSED, range.getEndBound());
    Assert.assertEquals(ByteString.copyFromUtf8("y"), range.getEnd());

    range = range.endOpen("x");
    Assert.assertEquals(BoundType.OPEN, range.getEndBound());
    Assert.assertEquals(ByteString.copyFromUtf8("x"), range.getEnd());
  }
}
