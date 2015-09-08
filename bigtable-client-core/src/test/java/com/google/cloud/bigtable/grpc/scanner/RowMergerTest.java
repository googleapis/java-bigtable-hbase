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
package com.google.cloud.bigtable.grpc.scanner;

import static com.google.cloud.bigtable.grpc.scanner.ReadRowTestUtils.createContentChunk;
import static com.google.cloud.bigtable.grpc.scanner.ReadRowTestUtils.createReadRowsResponse;
import static com.google.cloud.bigtable.grpc.scanner.ReadRowTestUtils.generateReadRowsResponses;
import static com.google.cloud.bigtable.grpc.scanner.ReadRowTestUtils.randomBytes;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

import com.google.bigtable.v1.Column;
import com.google.bigtable.v1.Family;
import com.google.bigtable.v1.ReadRowsResponse;
import com.google.bigtable.v1.ReadRowsResponse.Chunk;
import com.google.bigtable.v1.Row;
import com.google.protobuf.ByteString;

@RunWith(JUnit4.class)
public class RowMergerTest {


  private static final Chunk RESET_CHUNK = Chunk.newBuilder().setResetRow(true).build();
  private static final Chunk COMPLETE_CHUNK = Chunk.newBuilder().setCommitRow(true).build();

  private static final Chunk Family1_c1_CHUNK = createContentChunk("Family1", "c1", randomBytes(10),
    100L);
  private static final Chunk Family1_c2_CHUNK = createContentChunk("Family1", "c2", randomBytes(100),
    100L);
  private static final Chunk Family2_null_CHUNK = createContentChunk("Family2", null, null, 0L);

  @Rule
  public ExpectedException expectedException = ExpectedException.none();

  @Test
  public void resultsAreReadable() throws IOException {
    List<ReadRowsResponse> responses = generateReadRowsResponses("rowKey-%s", 3);
    List<Row> rows = new ArrayList<>();
    Iterator<ReadRowsResponse> iterator = responses.iterator();
    while (iterator.hasNext()) {
      rows.add(RowMerger.readNextRow(iterator));
    }
    Assert.assertEquals(responses.size(), rows.size());
    for (int i = 0; i < responses.size(); i++) {
      Assert.assertEquals(responses.get(i).getRowKey(), rows.get(i).getKey());
    }
  }

  @Test
  public void multipleChunksAreMerged() throws IOException {
    String rowKey = "row-1";

    ReadRowsResponse response = createReadRowsResponse(rowKey, Family1_c1_CHUNK);
    ReadRowsResponse response2 = createReadRowsResponse(rowKey, Family1_c2_CHUNK);
    ReadRowsResponse response3 = createReadRowsResponse(rowKey, Family2_null_CHUNK);
    ReadRowsResponse response4 = createReadRowsResponse(rowKey, COMPLETE_CHUNK);

    Iterator<ReadRowsResponse> iterator = getIterator(response, response2, response3, response4);

    Row resultRow = RowMerger.readNextRow(iterator);
    Assert.assertEquals(2, resultRow.getFamiliesCount());

    Map<String, Family> familyMap = new HashMap<>();
    for (Family family : resultRow.getFamiliesList()) {
      familyMap.put(family.getName(), family);
    }

    Family resultFamily1 = familyMap.get("Family1");
    Assert.assertEquals(2, resultFamily1.getColumnsCount());
    Assert.assertEquals(ByteString.copyFromUtf8("c1"),
        resultFamily1.getColumns(0).getQualifier());
    Assert.assertEquals(ByteString.copyFromUtf8("c2"),
        resultFamily1.getColumns(1).getQualifier());

    Family resultFamily2 = familyMap.get("Family2");
    Assert.assertEquals(0, resultFamily2.getColumnsCount());

    Assert.assertFalse(iterator.hasNext());
  }

  private Iterator<ReadRowsResponse> getIterator(ReadRowsResponse ... responses) {
    return Arrays.asList(responses).iterator();
  }

  @Test
  public void rowsMultipleChunks() throws IOException {
    String rowKey = "row-1";

    ReadRowsResponse multiChunkResponse =
        createReadRowsResponse(rowKey, Family1_c1_CHUNK, Family1_c2_CHUNK, COMPLETE_CHUNK);

    Iterator<ReadRowsResponse> iterator = getIterator(multiChunkResponse);

    Row resultRow = RowMerger.readNextRow(iterator);

    Assert.assertEquals(1, resultRow.getFamiliesCount());

    Family resultFamily = resultRow.getFamilies(0);
    Assert.assertEquals("Family1", resultFamily.getName());
    Assert.assertEquals(2, resultFamily.getColumnsCount());
    Assert.assertEquals(ByteString.copyFromUtf8("c1"), resultFamily.getColumns(0).getQualifier());
    Assert.assertEquals(ByteString.copyFromUtf8("c2"), resultFamily.getColumns(1).getQualifier());

    Assert.assertFalse(iterator.hasNext());
  }

  @Test
  public void readMultipleRows() throws IOException {
    ReadRowsResponse response = createReadRowsResponse("row-1", Family1_c1_CHUNK, COMPLETE_CHUNK);
    ReadRowsResponse response2 = createReadRowsResponse("row-2", Family1_c2_CHUNK, COMPLETE_CHUNK);

    Iterator<ReadRowsResponse> iterator = getIterator(response, response2);

    {
      Row resultRow = RowMerger.readNextRow(iterator);

      Assert.assertEquals("row-1", resultRow.getKey().toStringUtf8());
      Assert.assertEquals(1, resultRow.getFamiliesCount());

      Family resultFamily = resultRow.getFamilies(0);
      Assert.assertEquals("Family1", resultFamily.getName());
      Assert.assertEquals(1, resultFamily.getColumnsCount());
      Assert.assertEquals(ByteString.copyFromUtf8("c1"), resultFamily.getColumns(0).getQualifier());
    }

    Assert.assertTrue(iterator.hasNext());

    {
      Row resultRow = RowMerger.readNextRow(iterator);

      Assert.assertEquals("row-2", resultRow.getKey().toStringUtf8());
      Assert.assertEquals(1, resultRow.getFamiliesCount());

      Family resultFamily = resultRow.getFamilies(0);
      Assert.assertEquals("Family1", resultFamily.getName());
      Assert.assertEquals(1, resultFamily.getColumnsCount());
      Assert.assertEquals(ByteString.copyFromUtf8("c2"), resultFamily.getColumns(0).getQualifier());
    }

    Assert.assertFalse(iterator.hasNext());
  }

  @Test
  public void rowsCanBeReset() throws IOException {
    String rowKey = "row-1";

    ReadRowsResponse response = createReadRowsResponse(rowKey, Family1_c1_CHUNK);
    ReadRowsResponse response2 = createReadRowsResponse(rowKey, RESET_CHUNK);
    ReadRowsResponse response3 = createReadRowsResponse(rowKey, Family1_c2_CHUNK);
    ReadRowsResponse response4 = createReadRowsResponse(rowKey, COMPLETE_CHUNK);

    Iterator<ReadRowsResponse> iterator = getIterator(response, response2, response3, response4);

    Row resultRow = RowMerger.readNextRow(iterator);

    Assert.assertEquals(1, resultRow.getFamiliesCount());

    Family resultFamily = resultRow.getFamilies(0);
    Assert.assertEquals("Family1", resultFamily.getName());
    Assert.assertEquals(1, resultFamily.getColumnsCount());
    Column resultColumn = resultFamily.getColumns(0);
    Assert.assertEquals(ByteString.copyFromUtf8("c2"), resultColumn.getQualifier());

    Assert.assertFalse(iterator.hasNext());
  }

  @Test
  public void singleChunkRowsAreRead() throws IOException {
    String rowKey = "row-1";
    ReadRowsResponse response = createReadRowsResponse(rowKey, COMPLETE_CHUNK);

    final Iterator<ReadRowsResponse> iter = getIterator(response);
    Row resultRow = RowMerger.readNextRow(iter);
    Assert.assertEquals(0, resultRow.getFamiliesCount());
    Assert.assertEquals(ByteString.copyFromUtf8("row-1"), resultRow.getKey());

    Assert.assertFalse(iter.hasNext());
  }

  @Test
  public void endOfStreamMidRowThrows() throws IOException {
    String rowKey = "row-1";
    ReadRowsResponse response = createReadRowsResponse(rowKey, Family1_c1_CHUNK);

    expectedException.expectMessage("End of stream marker encountered while merging a row.");
    expectedException.expect(IllegalStateException.class);
    @SuppressWarnings("unused")
    Row resultRow = RowMerger.readNextRow(getIterator(response));
  }

  @Test
  public void oneRowPlusEndofStreamMidRowThrows() throws IOException {
    String rowKey = "row-1";
    ReadRowsResponse response = createReadRowsResponse(rowKey, Family1_c1_CHUNK, COMPLETE_CHUNK);
    ReadRowsResponse response2 = createReadRowsResponse(rowKey, Family1_c2_CHUNK);

    expectedException.expectMessage("End of stream marker encountered while merging a row.");
    expectedException.expect(IllegalStateException.class);
    final Iterator<ReadRowsResponse> iterator = getIterator(response, response2);
    @SuppressWarnings("unused")
    Row resultRow = RowMerger.readNextRow(iterator);
    resultRow = RowMerger.readNextRow(iterator);
  }
}
