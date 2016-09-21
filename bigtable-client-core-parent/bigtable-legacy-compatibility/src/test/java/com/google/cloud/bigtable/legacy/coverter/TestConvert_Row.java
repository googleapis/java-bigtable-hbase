/*
 * Copyright 2016 Google Inc. All Rights Reserved.
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
package com.google.cloud.bigtable.legacy.coverter;

import org.junit.Test;

import com.google.protobuf.ByteString;
import com.google.protobuf.InvalidProtocolBufferException;

import org.junit.Assert;

/**
 * Tests {@link BigtableLegacyProtobufConverter} for various types of {@link com.google.bigtable.v2.Row} scenarios.
 * @author sduskis
 *
 */

public class TestConvert_Row {

  @Test 
  public void testRow_default() throws InvalidProtocolBufferException {
    BigtableLegacyProtobufConverter.convert(com.google.bigtable.v2.Row.getDefaultInstance());
  }

  @Test
  public void testRow_full() throws InvalidProtocolBufferException {
    com.google.bigtable.v2.Cell v2Cell = com.google.bigtable.v2.Cell.newBuilder()
        .setTimestampMicros(1020222)
        .setValue(ByteString.copyFrom("value".getBytes()))
        .build();
    com.google.bigtable.v2.Column v2Column = com.google.bigtable.v2.Column.newBuilder()
        .setQualifier(ByteString.copyFrom("Qualifier".getBytes()))
        .addCells(v2Cell)
        .build();
    com.google.bigtable.v2.Family v2Family = com.google.bigtable.v2.Family.newBuilder()
        .setName("family")
        .addColumns(v2Column)
        .build();
    com.google.bigtable.v2.Row v2Row = com.google.bigtable.v2.Row.newBuilder()
        .setKey(ByteString.copyFrom("key".getBytes()))
        .addFamilies(v2Family)
        .build();

    com.google.bigtable.v1.Row v1Row = BigtableLegacyProtobufConverter.convert(v2Row);
    com.google.bigtable.v1.Family v1Family = v1Row.getFamilies(0);
    com.google.bigtable.v1.Column v1Column = v1Family.getColumns(0);
    com.google.bigtable.v1.Cell v1Cell = v1Column.getCells(0);

    Assert.assertEquals(v2Row.getKey(), v1Row.getKey());
    Assert.assertEquals(v2Family.getName(), v1Family.getName());
    Assert.assertEquals(v2Column.getQualifier(), v1Column.getQualifier());
    Assert.assertEquals(v2Cell.getValue(), v1Cell.getValue());
    Assert.assertEquals(v2Cell.getTimestampMicros(), v1Cell.getTimestampMicros());
  }
}
