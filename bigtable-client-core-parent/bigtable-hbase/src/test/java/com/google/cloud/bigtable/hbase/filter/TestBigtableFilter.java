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
package com.google.cloud.bigtable.hbase.filter;

import java.io.IOException;

import org.apache.hadoop.hbase.protobuf.ProtobufUtil;
import org.apache.hadoop.hbase.protobuf.generated.FilterProtos;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

import com.google.cloud.bigtable.data.v2.wrappers.Filters;

/**
 * Tests for {@link BigtableFilter}
 */
@RunWith(JUnit4.class)
public class TestBigtableFilter {
  @Test
  public void testSerialization() throws IOException {
    BigtableFilter original = new BigtableFilter(Filters.FILTERS.pass());
    FilterProtos.Filter proto = ProtobufUtil.toFilter(original);
    Assert.assertEquals(original, ProtobufUtil.toFilter(proto));
  }

}
