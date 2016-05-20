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

import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

import com.google.bigtable.repackaged.com.google.cloud.dataflow.tools.BigtableConverter;
import com.google.bigtable.repackaged.com.google.cloud.dataflow.tools.HBaseMutationConverter;
import com.google.bigtable.repackaged.com.google.cloud.dataflow.tools.HBaseResultArrayConverter;
import com.google.bigtable.repackaged.com.google.cloud.dataflow.tools.HBaseResultConverter;
import com.google.cloud.dataflow.sdk.util.SerializableUtils;

/**
 * Tests to make sure that various artifacts are serializable
 *
 */
@RunWith(JUnit4.class)
public class SerializableTest {

  @SuppressWarnings({ "rawtypes", "unchecked" })
  @Test
  public void testHBaseConvertersAreSerializable() {
    for (BigtableConverter converter : Arrays.asList(
          new HBaseMutationConverter(),
          new HBaseResultConverter(),
          new HBaseResultArrayConverter())) {
      SerializableUtils.ensureSerializable(new BigtableConverterCoder<>(converter));
    }
  }

  public void testScanConverterIsSerializable() {
    SerializableUtils.ensureSerializable(new CloudBigtableScanConfiguration.Builder().build());
  }
}
