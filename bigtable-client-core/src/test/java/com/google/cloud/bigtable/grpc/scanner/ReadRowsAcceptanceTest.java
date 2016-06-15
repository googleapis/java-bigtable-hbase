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

import com.google.bigtable.v2.Cell;
import com.google.bigtable.v2.Column;
import com.google.bigtable.v2.Family;
import com.google.bigtable.v2.ReadRowsResponse;
import com.google.bigtable.v2.ReadRowsResponse.CellChunk;
import com.google.cloud.bigtable.grpc.scanner.RowMerger;
import com.google.bigtable.v2.Row;
import com.google.common.base.CaseFormat;
import com.google.common.base.MoreObjects;
import com.google.protobuf.ByteString;
import com.google.protobuf.TextFormat;

import com.fasterxml.jackson.databind.ObjectMapper;

import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;

import io.grpc.stub.StreamObserver;

import java.io.IOException;
import java.io.InputStream;
import java.io.StringReader;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Objects;

/**
 * Parses and runs the acceptance tests for read rows
 */
@RunWith(Parameterized.class)
public class ReadRowsAcceptanceTest {
  // The acceptance test data model, populated via jackson data binding
  private static final class AcceptanceTest {
    public List<ChunkTestCase> tests;
  }

  private static final class ChunkTestCase {
    public String name;
    public List<String> chunks;
    public List<TestResult> results;

    /**
     * The test name in the source file is an arbitrary string. Make it junit-friendly.
     */
    public String getJunitTestName() {
      return CaseFormat.LOWER_HYPHEN.to(CaseFormat.LOWER_CAMEL, name.replace(" ", "-"));
    }

    @Override
    public String toString() {
      return getJunitTestName();
    }

    public boolean expectsError() {
      return results != null && !results.isEmpty() && results.get(results.size() - 1).error;
    }

    public List<TestResult> getNonExceptionResults() {
      ArrayList<TestResult> response = new ArrayList<>();
      if (results != null) {
        for (TestResult result : results) {
          if (!result.error) {
            response.add(result);
          }
        }
      }
      return response;
    }
  }

  private final static class TestResult {
    public String rk;
    public String fm;
    public String qual;
    public long ts;
    public String value;
    public String label;
    public boolean error;

    /**
     * Constructor for JSon deserialization.
     */
    @SuppressWarnings("unused")
    public TestResult() {
    }

    public TestResult(
        String rk, String fm, String qual, long ts, String value, String label, boolean error) {
      this.rk = rk;
      this.fm = fm;
      this.qual = qual;
      this.ts = ts;
      this.value = value;
      this.label = label;
      this.error = error;
    }

    @Override
    public String toString() {
      return MoreObjects.toStringHelper(this)
          .add("rk", rk)
          .add("fm", fm)
          .add("qual", qual)
          .add("ts", ts)
          .add("value", value)
          .add("label", label)
          .add("error", error)
          .toString();
    }

    @Override
    public boolean equals(Object obj) {
      if (obj == null || !(obj instanceof TestResult)){
        return false;
      }
      if (obj == this) {
        return true;
      }
      TestResult other = (TestResult) obj;
      return Objects.equals(rk, other.rk)
          && Objects.equals(fm, other.fm)
          && Objects.equals(qual, other.qual)
          && Objects.equals(ts, other.ts)
          && Objects.equals(value, other.value)
          && Objects.equals(label, other.label)
          && Objects.equals(error, other.error);
    }
  }

  private final ChunkTestCase testCase;

  @Parameters(name = "{0}")
  public static Collection<Object[]> data() {
    InputStream testInputStream = ReadRowsAcceptanceTest.class
        .getResourceAsStream("read-rows-acceptance-test.json");

    ObjectMapper mapper = new ObjectMapper();
    try {
      AcceptanceTest acceptanceTest = mapper.readValue(testInputStream, AcceptanceTest.class);
      List<Object[]> data = new ArrayList<>();
      for (ChunkTestCase test : acceptanceTest.tests) {
        data.add(new Object[]{ test });
      }
      return data;
    } catch (IOException e) {
      throw new RuntimeException("Error loading acceptance test file", e);
    }
  }

  public ReadRowsAcceptanceTest(ChunkTestCase testCase) {
    this.testCase = testCase;
  }

  @Test
  public void test() throws Exception {
    // TODO Merge the specified chunks into rows and
    // validate the returned rows against the test results.
    List<Row> responses = new ArrayList<>();
    List<Throwable> exceptions = new ArrayList<>();
    addResponses(responses, exceptions);
    processResults(responses, exceptions);
  }

  private void addResponses(List<Row> responses, List<Throwable> exceptions) throws IOException {
    RowMerger rowMerger = createRowMerger(responses, exceptions);
    ReadRowsResponse.Builder responseBuilder = ReadRowsResponse.newBuilder();
    for (String chunkStr : testCase.chunks) {
      CellChunk.Builder ccBuilder = CellChunk.newBuilder();
      TextFormat.merge(new StringReader(chunkStr), ccBuilder);
      responseBuilder.addChunks(ccBuilder.build());
    }
    rowMerger.onNext(responseBuilder.build());
    if (exceptions.isEmpty()) {
      rowMerger.onCompleted();
    }
  }

  private static RowMerger createRowMerger(
      final List<Row> responses, final List<Throwable> exceptions) {
    return new RowMerger(
        new StreamObserver<Row>() {

          @Override
          public void onNext(Row value) {
            responses.add(value);
          }

          @Override
          public void onError(Throwable t) {
            exceptions.add(t);
          }

          @Override
          public void onCompleted() {}
        });
  }

  private void processResults(List<Row> responses, List<Throwable> exceptions) {
    List<TestResult> denormalizedResponses = denormalizeResponses(responses);
    Assert.assertEquals(testCase.getNonExceptionResults(), denormalizedResponses);
    if (testCase.expectsError()) {
      if (exceptions.isEmpty()) {
        Assert.fail("expected exception");
      }
    } else if (!exceptions.isEmpty()) {
      Assert.fail("Got unexpected exception: " + exceptions.get(exceptions.size() - 1).getMessage());
    }
  }

  private List<TestResult> denormalizeResponses(List<Row> responses) {
    ArrayList<TestResult> response = new ArrayList<>();
    for (Row row : responses) {
      for (Family family : row.getFamiliesList()) {
        for (Column column : family.getColumnsList()) {
          for (Cell cell : column.getCellsList()) {
            response.add(new TestResult(
              toString(row.getKey()),
              family.getName(),
              toString(column.getQualifier()),
              cell.getTimestampMicros(),
              toString(cell.getValue()),
              cell.getLabelsCount() == 0 ? "" : cell.getLabels(0),
              false));
          }
        }
      }
    }
    return response;
  }

  protected String toString(final ByteString byteString) {
    return new String(byteString.toByteArray());
  }
}
