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
package com.google.cloud.bigtable.dataflow.tools;

import static org.apache.hadoop.hbase.util.Bytes.toBytes;

import java.io.IOException;

import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Put;
import org.junit.Assert;
import org.junit.Test;

/**
 * Tests for {@link HBaseMutationCoder}.
 */
public class HBaseMutationCoderTest {

  private HBaseMutationConverter underTest = new HBaseMutationConverter();

  @Test
  public void testPut() throws IOException {
    Put original = new Put(toBytes("key")).addColumn(toBytes("family"), toBytes("column"), toBytes("value"));
    Assert.assertEquals(0, original.compareTo(CoderTestUtil.encodeAndDecode(underTest, original)));
  }

  @Test
  public void testDelete() throws IOException {
    Delete original = new Delete(toBytes("key"));
    Assert.assertEquals(0, original.compareTo(CoderTestUtil.encodeAndDecode(underTest, original)));
  }
}
