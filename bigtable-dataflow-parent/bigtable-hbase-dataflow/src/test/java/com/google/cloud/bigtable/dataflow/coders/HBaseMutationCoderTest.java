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

import static org.apache.hadoop.hbase.util.Bytes.toBytes;

import com.google.bigtable.repackaged.com.google.api.client.util.Clock;
import com.google.bigtable.repackaged.com.google.cloud.bigtable.hbase.adapters.PutAdapterUtil;
import com.google.cloud.dataflow.sdk.util.MutationDetector;
import com.google.cloud.dataflow.sdk.util.MutationDetectors;
import java.io.IOException;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Put;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

/**
 * Tests for {@link HBaseMutationCoder}.
 */
public class HBaseMutationCoderTest {

  private HBaseMutationCoder underTest;
  private AtomicLong time;

  @Before
  public void setup() {
    underTest = new HBaseMutationCoder();
    time = new AtomicLong(System.currentTimeMillis());

    PutAdapterUtil.setClock(HBaseMutationCoder.PUT_ADAPTER, new Clock(){
      @Override
      public long currentTimeMillis() {
        return time.get();
      }
    });
  }

  @After
  public void tearDown() {
    PutAdapterUtil.setClock(HBaseMutationCoder.PUT_ADAPTER, Clock.SYSTEM);
  }

  @Test
  public void testPut() throws IOException {
    Put original =
        new Put(toBytes("key")).addColumn(toBytes("family"), toBytes("column"), toBytes("value"));
    MutationDetector mutationDetector = MutationDetectors.forValueWithCoder(original, underTest);
    for (int i = 0; i < 5; i++) {
      Assert.assertEquals(
          0, original.compareTo(CoderTestUtil.encodeAndDecode(underTest, original)));
      time.set(time.get() + 10_000);
      Assert.assertEquals(
          0, original.compareTo(CoderTestUtil.encodeAndDecode(underTest, original)));

      // Make sure that the clock change didn't modify the serialized value.
      mutationDetector.verifyUnmodified();
    }
  }

  @Test
  public void testDelete() throws IOException {
    Delete original = new Delete(toBytes("key"));
    MutationDetector mutationDetector = MutationDetectors.forValueWithCoder(original, underTest);
    for (int i = 0; i < 5; i++) {
      Assert.assertEquals(
          0, original.compareTo(CoderTestUtil.encodeAndDecode(underTest, original)));
      time.set(time.get() + 10_000);

      // Make sure that the clock change didn't modify the serialized value.
      mutationDetector.verifyUnmodified();
    }
  }
}
