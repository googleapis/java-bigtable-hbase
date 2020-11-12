/*
 * Copyright 2019 Google LLC
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
package com.google.cloud.bigtable.hbase.util;

import java.util.concurrent.TimeUnit;
import org.apache.hadoop.hbase.HConstants;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
public class TestTimestampConverter {

  @Test
  public void testNow() {
    long nowMs = System.currentTimeMillis();
    long nowMicros = TimeUnit.MILLISECONDS.toMicros(nowMs);

    Assert.assertEquals(nowMs, TimestampConverter.bigtable2hbase(nowMicros));
    Assert.assertEquals(nowMicros, TimestampConverter.hbase2bigtable(nowMs));
  }

  @Test
  public void testLarge() {
    Assert.assertEquals(
        HConstants.LATEST_TIMESTAMP, TimestampConverter.bigtable2hbase(Long.MAX_VALUE));
    Assert.assertEquals(
        TimestampConverter.BIGTABLE_MAX_TIMESTAMP,
        TimestampConverter.hbase2bigtable(Long.MAX_VALUE));
  }
}
