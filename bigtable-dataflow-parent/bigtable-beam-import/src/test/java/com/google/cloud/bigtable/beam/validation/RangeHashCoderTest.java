/*
 * Copyright 2021 Google LLC
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
package com.google.cloud.bigtable.beam.validation;

import com.google.cloud.bigtable.beam.validation.HadoopHashTableSource.RangeHash;
import org.apache.beam.sdk.coders.CoderException;
import org.apache.beam.sdk.testing.CoderProperties;
import org.apache.beam.sdk.util.CoderUtils;
import org.apache.beam.sdk.values.TypeDescriptor;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.junit.Assert;
import org.junit.Test;

public class RangeHashCoderTest {
  private static final RangeHashCoder TEST_CODER = new RangeHashCoder();
  private static final ImmutableBytesWritable START =
      new ImmutableBytesWritable("Start".getBytes());
  private static final ImmutableBytesWritable STOP = new ImmutableBytesWritable("Stop".getBytes());
  private static final ImmutableBytesWritable HASH = new ImmutableBytesWritable("hash".getBytes());
  private static final ImmutableBytesWritable EMPTY =
      new ImmutableBytesWritable(HConstants.EMPTY_BYTE_ARRAY);

  @Test
  public void encodeRangeHash() throws Exception {
    CoderProperties.coderDecodeEncodeEqual(TEST_CODER, RangeHash.of(START, STOP, HASH));
  }

  @Test(expected = CoderException.class)
  public void encodeNullThrowsCoderException() throws Exception {
    CoderUtils.encodeToByteArray(TEST_CODER, null);
  }

  @Test
  public void testEncodedTypeDescriptor() throws Exception {
    Assert.assertEquals(TEST_CODER.getEncodedTypeDescriptor(), TypeDescriptor.of(RangeHash.class));
  }
}
