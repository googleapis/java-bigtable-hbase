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

import org.junit.Assert;
import org.junit.Test;

import com.google.bigtable.v1.Mutation.DeleteFromRow;
import com.google.bigtable.v2.Mutation.MutationCase;
import com.google.protobuf.ByteString;
import com.google.protobuf.InvalidProtocolBufferException;

/**
 * Tests {@link BigtableLegacyProtobufConverter} for various types of {@link com.google.bigtable.v1.Mutation} scenarios.
 * @author sduskis
 *
 */
public class TestConvert_Mutations {

  @Test
  public void testDefaultMutation() throws InvalidProtocolBufferException {
    BigtableLegacyProtobufConverter.convert(com.google.bigtable.v1.Mutation.getDefaultInstance());
  }

  @Test
  public void testSetCell() throws InvalidProtocolBufferException {
    com.google.bigtable.v1.Mutation.SetCell v1SetCell =
        com.google.bigtable.v1.Mutation.SetCell.newBuilder().setFamilyName("Family")
            .setColumnQualifier(ByteString.copyFrom("Qualifier".getBytes()))
            .setTimestampMicros(100000000)
            .setValue(ByteString.copyFrom("Some Random Value".getBytes())).build();

    com.google.bigtable.v2.Mutation.SetCell v2SetCell = BigtableLegacyProtobufConverter
        .convert(com.google.bigtable.v1.Mutation.newBuilder().setSetCell(v1SetCell).build())
        .getSetCell();

    Assert.assertEquals(v1SetCell.getFamilyName(), v2SetCell.getFamilyName());
    Assert.assertEquals(v1SetCell.getColumnQualifier(), v2SetCell.getColumnQualifier());
    Assert.assertEquals(v1SetCell.getTimestampMicros(), v2SetCell.getTimestampMicros());
    Assert.assertEquals(v1SetCell.getTimestampMicros(), v2SetCell.getTimestampMicros());
    Assert.assertEquals(v1SetCell.getValue(), v2SetCell.getValue());
  }

  @Test
  public void testDeleteFromRow() throws InvalidProtocolBufferException {
    com.google.bigtable.v2.Mutation mutation = BigtableLegacyProtobufConverter.convert(com.google.bigtable.v1.Mutation
        .newBuilder().setDeleteFromRow(DeleteFromRow.getDefaultInstance()).build());
    Assert.assertEquals(MutationCase.DELETE_FROM_ROW, mutation.getMutationCase());
  }

  public void testDeleteFromColumn() throws InvalidProtocolBufferException {
    com.google.bigtable.v1.Mutation.DeleteFromColumn v1DeleteFromColumn =
        com.google.bigtable.v1.Mutation.DeleteFromColumn.newBuilder().setFamilyName("Family")
            .setColumnQualifier(ByteString.copyFrom("Qualifier".getBytes())).build();

    com.google.bigtable.v2.Mutation.DeleteFromColumn v2DeleteFromColumn = BigtableLegacyProtobufConverter.convert(
      com.google.bigtable.v1.Mutation.newBuilder().setDeleteFromColumn(v1DeleteFromColumn).build())
        .getDeleteFromColumn();

    Assert.assertEquals(v1DeleteFromColumn.getFamilyName(), v2DeleteFromColumn.getFamilyName());
    Assert.assertEquals(v1DeleteFromColumn.getColumnQualifier(),
      v2DeleteFromColumn.getColumnQualifier());
  }
}
