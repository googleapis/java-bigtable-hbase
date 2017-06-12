/*
 * Copyright 2015 Google Inc. All Rights Reserved.
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
package com.google.cloud.bigtable.dataflow;

import java.util.Arrays;

import org.apache.beam.sdk.coders.AtomicCoder;
import org.apache.beam.sdk.util.SerializableUtils;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

import com.google.cloud.bigtable.dataflow.coders.HBaseMutationCoder;
import com.google.cloud.bigtable.dataflow.coders.HBaseResultArrayCoder;
import com.google.cloud.bigtable.dataflow.coders.HBaseResultCoder;

/**
 * Tests to make sure that various artifacts are serializable
 *
 */
@RunWith(JUnit4.class)
public class SerializableTest {

  @Test
  public void testHBaseConvertersAreSerializable() {
    for (AtomicCoder<? extends Object> coder : Arrays.asList(
          new HBaseMutationCoder(),
          new HBaseResultCoder(),
          new HBaseResultArrayCoder())) {
      SerializableUtils.ensureSerializable(coder);
    }
  }

  public void testScanConverterIsSerializable() {
    SerializableUtils.ensureSerializable(new CloudBigtableScanConfiguration.Builder().build());
  }
}
