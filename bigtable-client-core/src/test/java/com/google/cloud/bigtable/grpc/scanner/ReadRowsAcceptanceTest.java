package com.google.cloud.bigtable.grpc.scanner;

import com.google.bigtable.v2.Cell;
import com.google.bigtable.v2.Family;
import com.google.bigtable.v2.Family.Builder;
import com.google.bigtable.v2.ReadRowsResponse.CellChunk;
import com.google.bigtable.v2.Row;
import com.google.common.base.CaseFormat;
import com.google.protobuf.ByteString;
import com.google.protobuf.TextFormat;

import com.fasterxml.jackson.databind.ObjectMapper;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;

import java.io.IOException;
import java.io.InputStream;
import java.io.StringReader;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

/**
 * Parses and runs the acceptance tests for read rows
 */
@RunWith(Parameterized.class)
public class ReadRowsAcceptanceTest {
  // The acceptance test data model, populated via jackson data binding
  private static final class AcceptanceTest {
    public List<TestCase> tests;
  }

  private static final class TestCase {
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
  }

  private final static class TestResult {
    public String rk;
    public String fm;
    public String qual;
    public long ts;
    public String value;
    public String label;
    public boolean error;
  }

  private final TestCase testCase;

  @Parameters(name = "{0}")
  public static Collection<Object[]> data() {
    InputStream testInputStream = ReadRowsAcceptanceTest.class
        .getResourceAsStream("read-rows-acceptance-test.json");

    ObjectMapper mapper = new ObjectMapper();
    try {
      AcceptanceTest acceptanceTest = mapper.readValue(testInputStream, AcceptanceTest.class);
      List<Object[]> data = new ArrayList<>();
      for (TestCase test : acceptanceTest.tests) {
        data.add(new Object[]{ test });
      }
      return data;
    } catch (IOException e) {
      throw new RuntimeException("Error loading acceptance test file", e);
    }
  }

  public ReadRowsAcceptanceTest(TestCase testCase) {
    this.testCase = testCase;
  }

  @Test
  public void test() throws Exception {
    // TODO Merge the specified chunks into rows and
    // validate the returned rows against the test results.
    for (String chunkStr : testCase.chunks) {
      CellChunk.Builder ccBuilder = CellChunk.newBuilder();
      TextFormat.merge(new StringReader(chunkStr), ccBuilder);
      CellChunk cc = ccBuilder.build();
    }
  }

  private Row buildRow(TestResult result) {
    Row.Builder rowBuilder = Row.newBuilder();
    rowBuilder.setKey(ByteString.copyFromUtf8(result.rk));
    Builder familyBuilder = Family.newBuilder().setName(result.fm);

    Cell.Builder cellBuilder = Cell.newBuilder();
    cellBuilder.setValue(ByteString.copyFromUtf8(result.value));
    cellBuilder.setTimestampMicros(result.ts);
    if (result.label != null && !result.label.equals("")) {
      cellBuilder.addLabels(result.label);
    }

    return rowBuilder.build();
  }
}
