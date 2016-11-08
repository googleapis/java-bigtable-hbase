/*
 * Copyright (C) 2015 Google Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */
package com.google.cloud.bigtable.dataflow.coders;

import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.client.Result;
import org.junit.Assert;
import org.junit.Test;

import com.google.bigtable.repackaged.com.google.cloud.hbase.adapters.read.RowCell;
import com.google.cloud.dataflow.sdk.util.CoderUtils;

/**
 * Tests for {@link HBaseResultArrayCoder}
 */
public class HBaseResultArrayCoderTest {

  private HBaseResultArrayCoder underTest = new HBaseResultArrayCoder();

  private static Result[] original = new Result[] { createResult() };

  @Test
  public void testRoundTrip() throws Exception {
    Result[] copy = CoderUtils.decodeFromByteArray(underTest,
      CoderUtils.encodeToByteArray(underTest, original));
    // This method throws an exception if the values are not equal.
    Result.compareResults(original[0], copy[0]);
  }

  private static Result createResult() {
    return Result.create(new Cell[] { new RowCell("key".getBytes(), "family".getBytes(),
        "qualifier".getBytes(), System.currentTimeMillis(), "value".getBytes()) });
  }

  @Test
  public void ensureDeterministic() throws Exception {
    Assert.assertArrayEquals(
      CoderUtils.encodeToByteArray(underTest, original),
      CoderUtils.encodeToByteArray(underTest, original));
  }
}

