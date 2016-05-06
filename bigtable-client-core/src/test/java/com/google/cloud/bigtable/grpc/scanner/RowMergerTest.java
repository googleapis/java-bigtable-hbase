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
import static com.google.cloud.bigtable.grpc.scanner.RowMatcher.matchesRow;

import java.util.Arrays;
import java.util.List;

import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

import com.google.bigtable.v1.ReadRowsResponse;
import com.google.bigtable.v1.ReadRowsResponse.Chunk;
import com.google.bigtable.v1.Row;

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
  public void resultsAreReadable() {
    List<ReadRowsResponse> responses = generateReadRowsResponses("rowKey-%s", 3);
    List<Row> rows = RowMerger.toRows(responses);
    Assert.assertEquals(responses.size(), rows.size());
    for (int i = 0; i < responses.size(); i++) {
      Assert.assertEquals(responses.get(i).getRowKey(), rows.get(i).getKey());
    }
  }

  private void matchResponses(ReadRowsResponse[] responses, RowMatcher[] expectedRows) {
    int i = 0;
    for(Row row : RowMerger.toRows(Arrays.asList(responses))) {
      if(row != null) {
        Assert.assertFalse("Responses not completely consumed by expected rows", i >= expectedRows.length);
        Assert.assertThat("Expected row does not match!", row, expectedRows[i]);
        i++;
      }
    }
    Assert.assertTrue("More expected rows than supplied responses", expectedRows.length == i);
  }

  @Test
  public void multipleChunksAreMerged() {
    matchResponses(
        new ReadRowsResponse[]{
            createReadRowsResponse("row-1", Family1_c1_CHUNK),
            createReadRowsResponse("row-1", Family1_c2_CHUNK),
            createReadRowsResponse("row-1", Family2_null_CHUNK),
            createReadRowsResponse("row-1", COMPLETE_CHUNK)
        },
        new RowMatcher[]{
            matchesRow("row-1")
                .withFamily("Family1")
                    .withColumn("c1").withAnyCell()
                    .withColumn("c2").withAnyCell()
                .withFamily("Family2")
        });
  }

  @Test
  public void rowsMultipleChunks() {
    matchResponses(
        new ReadRowsResponse[]{
            createReadRowsResponse("row-1", Family1_c1_CHUNK, Family1_c2_CHUNK, COMPLETE_CHUNK)
        },
        new RowMatcher[]{
            matchesRow("row-1").withFamily("Family1")
                .withColumn("c1").withAnyCell()
                .withColumn("c2").withAnyCell()
        });
  }

  @Test
  public void readMultipleRows() {
    matchResponses(
        new ReadRowsResponse[]{
            createReadRowsResponse("row-1", Family1_c1_CHUNK, COMPLETE_CHUNK),
            createReadRowsResponse("row-2", Family1_c2_CHUNK, COMPLETE_CHUNK)
        },
        new RowMatcher[]{
            matchesRow("row-1").withFamily("Family1").withColumn("c1").withAnyCell(),
            matchesRow("row-2").withFamily("Family1").withColumn("c2").withAnyCell()
        });
  }

  @Test
  public void rowsCanBeReset() {
    matchResponses(
        new ReadRowsResponse[]{
            createReadRowsResponse("row-1", Family1_c1_CHUNK),
            createReadRowsResponse("row-1", RESET_CHUNK),
            createReadRowsResponse("row-1", Family1_c2_CHUNK),
            createReadRowsResponse("row-1", COMPLETE_CHUNK),
            // Second row same as the first, except in 2 responses instead of 4.
            createReadRowsResponse("row-2", Family1_c1_CHUNK),
            createReadRowsResponse("row-2", RESET_CHUNK, Family1_c2_CHUNK, COMPLETE_CHUNK),
            // Same, but with slightly different grouping into responses.
            createReadRowsResponse("row-3", Family1_c1_CHUNK),
            createReadRowsResponse("row-3", RESET_CHUNK, Family1_c2_CHUNK),
            createReadRowsResponse("row-3", COMPLETE_CHUNK),
            // An initial reset with no preceding data shouldn't happen but is OK.
            createReadRowsResponse("row-4", RESET_CHUNK, Family1_c1_CHUNK),
            createReadRowsResponse("row-4", COMPLETE_CHUNK),
        },
        new RowMatcher[]{
            matchesRow("row-1").withFamily("Family1").withColumn("c2").withAnyCell(),
            matchesRow("row-2").withFamily("Family1").withColumn("c2").withAnyCell(),
            matchesRow("row-3").withFamily("Family1").withColumn("c2").withAnyCell(),
            matchesRow("row-4").withFamily("Family1").withColumn("c1").withAnyCell(),
        });
  }

  @Test
  public void rowsCanBeResetMultipleTimes() {
    matchResponses(
        new ReadRowsResponse[]{
            createReadRowsResponse("row-1", Family1_c1_CHUNK),
            createReadRowsResponse("row-1", RESET_CHUNK),
            createReadRowsResponse("row-1", Family1_c2_CHUNK),
            createReadRowsResponse("row-1", RESET_CHUNK, Family1_c1_CHUNK, COMPLETE_CHUNK),
            // Same, but with slightly different grouping into responses.
            createReadRowsResponse("row-2", Family1_c1_CHUNK),
            createReadRowsResponse("row-2", RESET_CHUNK, Family1_c2_CHUNK),
            createReadRowsResponse("row-2", RESET_CHUNK, Family1_c1_CHUNK, COMPLETE_CHUNK),
        },
        new RowMatcher[]{
            matchesRow("row-1").withFamily("Family1").withColumn("c1").withAnyCell(),
            matchesRow("row-2").withFamily("Family1").withColumn("c1").withAnyCell()
        });
  }

  @Test
  public void resetCompleteRowsAreRead() {
    // All of these responses indicate a delete. RowMerger should not return any Row objects for
    // these cases
    matchResponses(
        new ReadRowsResponse[]{
            createReadRowsResponse("row-1", COMPLETE_CHUNK),
            // Data and a reset, followed by a single commit (in various groupings).
            createReadRowsResponse("row-2", Family1_c1_CHUNK, RESET_CHUNK, COMPLETE_CHUNK),
            createReadRowsResponse("row-3", Family1_c1_CHUNK),
            createReadRowsResponse("row-3", RESET_CHUNK, COMPLETE_CHUNK),
            createReadRowsResponse("row-4", Family1_c1_CHUNK),
            createReadRowsResponse("row-4", RESET_CHUNK),
            createReadRowsResponse("row-4", COMPLETE_CHUNK),
            createReadRowsResponse("row-5", Family1_c1_CHUNK, RESET_CHUNK),
            createReadRowsResponse("row-5", COMPLETE_CHUNK),
            createReadRowsResponse("row-6", RESET_CHUNK, Family1_c1_CHUNK, RESET_CHUNK),
            createReadRowsResponse("row-6", RESET_CHUNK, Family1_c1_CHUNK, RESET_CHUNK),
            createReadRowsResponse("row-6", COMPLETE_CHUNK),
            createReadRowsResponse("row-7", RESET_CHUNK, COMPLETE_CHUNK),
        },
        new RowMatcher[0]);
  }

  @Test
  public void endOfStreamMidRowThrows() {
    String rowKey = "row-1";
    ReadRowsResponse response = createReadRowsResponse(rowKey, Family1_c1_CHUNK);

    expectedException.expectMessage("End of stream marker encountered while merging a row.");
    expectedException.expect(IllegalStateException.class);
    RowMerger.toRows(Arrays.asList(response));
  }

  @Test
  public void oneRowPlusEndofStreamMidRowThrows() {
    String rowKey = "row-1";
    ReadRowsResponse response = createReadRowsResponse(rowKey, Family1_c1_CHUNK, COMPLETE_CHUNK);
    ReadRowsResponse response2 = createReadRowsResponse(rowKey, Family1_c2_CHUNK);

    expectedException.expectMessage("End of stream marker encountered while merging a row.");
    expectedException.expect(IllegalStateException.class);
    RowMerger.toRows(Arrays.asList(response, response2));
  }
}
