/*
 * Copyright 2018 Google Inc. All Rights Reserved.
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
package com.google.cloud.bigtable.hbase.adapters;

import static com.google.cloud.bigtable.data.v2.models.Filters.FILTERS;
import com.google.bigtable.v2.CheckAndMutateRowRequest;
import com.google.bigtable.v2.Mutation;
import com.google.bigtable.v2.RowFilter;
import com.google.cloud.bigtable.data.v2.internal.RequestContext;
import com.google.cloud.bigtable.data.v2.models.ConditionalRowMutation;
import com.google.cloud.bigtable.data.v2.models.Filters;
import com.google.cloud.bigtable.data.v2.models.InstanceName;
import com.google.cloud.bigtable.grpc.BigtableInstanceName;
import com.google.cloud.bigtable.grpc.BigtableTableName;
import com.google.protobuf.ByteString;
import org.apache.hadoop.hbase.DoNotRetryIOException;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.RowMutations;
import org.apache.hadoop.hbase.filter.CompareFilter.CompareOp;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.TimeUnit;

@RunWith(JUnit4.class)
public class TestCheckAndMutateUtil {

  private static final TableName TABLE_NAME = TableName.valueOf("SomeTable");
  private static final BigtableTableName BT_TABLE_NAME =
      new BigtableInstanceName("project", "instance")
          .toTableName(TABLE_NAME.getNameAsString());
  private static final RequestContext REQUEST_CONTEXT =
      RequestContext
          .create(InstanceName.of(BT_TABLE_NAME.getProjectId(), BT_TABLE_NAME.getInstanceId()), "SomeAppProfileId");
  private static final byte[] rowKey = Bytes.toBytes("rowKey");
  private static final byte[] family = Bytes.toBytes("family");
  private static final byte[] qual = Bytes.toBytes("qual");
  private static final byte[] checkValue = Bytes.toBytes(4);
  private static final byte[] newValue = Bytes.toBytes(5);
  private static final Put PUT = new Put(rowKey).addColumn(family, qual, newValue);
  private static final Filters.ChainFilter FAMILY_AND_QUAL_FILTER = FILTERS.chain()
      .filter(FILTERS.family().regex("family"))
      .filter(FILTERS.qualifier().regex("qual"));

  private static HBaseRequestAdapter requestAdapter;

  @BeforeClass
  public static void setup() {
    PutAdapter putAdapter = new PutAdapter(100, true);
    HBaseRequestAdapter.MutationAdapters mutationAdapters =
        new HBaseRequestAdapter.MutationAdapters(putAdapter);
    requestAdapter = new HBaseRequestAdapter(
        TABLE_NAME,
        BT_TABLE_NAME,
        mutationAdapters,
        RequestContext.create(
            InstanceName.of(BT_TABLE_NAME.getProjectId(), BT_TABLE_NAME.getInstanceId()),
            "APP_PROFILE_ID"
        ));
  }

  private static void checkPredicate(CheckAndMutateRowRequest result) {
    RowFilter expected = FILTERS.chain()
        .filter(FAMILY_AND_QUAL_FILTER)
        .filter(FILTERS.limit().cellsPerColumn(1))
        .filter(FILTERS.value().range()
            .startClosed(ByteString.copyFrom(checkValue))
            .endClosed(ByteString.copyFrom(checkValue))
        )
        .toProto();
    Assert.assertEquals(expected, result.getPredicateFilter());
  }

  private static void checkPutMutation(Mutation mutation) {
    Mutation.SetCell setCell = mutation.getSetCell();
    Assert.assertArrayEquals(family, setCell.getFamilyNameBytes().toByteArray());
    Assert.assertArrayEquals(qual, setCell.getColumnQualifier().toByteArray());
    Assert.assertArrayEquals(newValue, setCell.getValue().toByteArray());
  }


  private static CheckAndMutateUtil.RequestBuilder createRequestBuilder() {
    return new CheckAndMutateUtil.RequestBuilder(requestAdapter, rowKey, family);
  }

  //  ************************** TEST METHODS ***************************

  @Test
  /** Tests that a CheckAndMutate with a {@link Put} works correctly for the filter and mutation aspects */
  public void testPut() throws DoNotRetryIOException {
    CheckAndMutateUtil.RequestBuilder underTest = createRequestBuilder();

    ConditionalRowMutation conditionalRowMutation = underTest
        .qualifier(qual)
        .ifMatches(CompareOp.EQUAL, checkValue)
        .withPut(PUT)
        .build();

    CheckAndMutateRowRequest result = conditionalRowMutation.toProto(REQUEST_CONTEXT);
    Assert.assertEquals(1, result.getTrueMutationsCount());

    checkPutMutation(result.getTrueMutations(0));
    checkPredicate(result);
  }

  @Test
  /** Tests that a CheckAndMutate with a {@link Delete} works correctly for the filter and mutation aspects */
  public void testDelete() throws DoNotRetryIOException {
    CheckAndMutateUtil.RequestBuilder underTest = createRequestBuilder();

    ConditionalRowMutation conditionalRowMutation = underTest
        .qualifier(qual)
        .ifMatches(CompareOp.EQUAL, checkValue)
        .withDelete(new Delete(rowKey).addColumns(family, qual))
        .build();

    CheckAndMutateRowRequest result = conditionalRowMutation.toProto(REQUEST_CONTEXT);
    Assert.assertEquals(1, result.getTrueMutationsCount());

    Mutation.DeleteFromColumn delete = result.getTrueMutations(0).getDeleteFromColumn();
    Assert.assertArrayEquals(family, delete.getFamilyNameBytes().toByteArray());
    Assert.assertArrayEquals(qual, delete.getColumnQualifier().toByteArray());

    checkPredicate(result);
  }

  @Test
  /** Tests that a CheckAndMutate with a {@link RowMutations} works correctly for the filter and mutation aspects */
  public void testRowMutations() throws IOException {
    CheckAndMutateUtil.RequestBuilder underTest = createRequestBuilder();

    RowMutations rowMutations = new RowMutations(rowKey);
    rowMutations.add(PUT);

    ConditionalRowMutation conditionalRowMutation = underTest
        .qualifier(qual)
        .ifMatches(CompareOp.EQUAL, checkValue)
        .withMutations(rowMutations)
        .build();

    CheckAndMutateRowRequest result = conditionalRowMutation.toProto(REQUEST_CONTEXT);
    Assert.assertEquals(1, result.getTrueMutationsCount());
    checkPutMutation(result.getTrueMutations(0));
    checkPredicate(result);
  }

  @Test
  /**
   * Tests that a CheckAndMutate with a {@link Put} which ensures that the conversion to a
   * {@link ConditionalRowMutation} sets a server-side timeatamp (-1) on the {@link com.google.cloud.bigtable.data.v2.models.Mutation}
   */
  public void testPutServerSideTimestamps() throws DoNotRetryIOException {
    CheckAndMutateUtil.RequestBuilder underTest = createRequestBuilder();

    ConditionalRowMutation conditionalRowMutation = underTest
        .qualifier(qual)
        .ifMatches(CompareOp.EQUAL, checkValue)
        .withPut(PUT)
        .build();

    CheckAndMutateRowRequest result = conditionalRowMutation.toProto(REQUEST_CONTEXT);
    Assert.assertEquals(1, result.getTrueMutationsCount());

    Mutation.SetCell setCell = result.getTrueMutations(0).getSetCell();
    Assert.assertEquals(-1, setCell.getTimestampMicros());
  }

  @Test
  /**
   * Tests that a CheckAndMutate with a {@link Put} which ensures that the conversion to a
   * {@link ConditionalRowMutation} sets a clientr-side timeatamp on the {@link com.google.cloud.bigtable.data.v2.models.Mutation}
   * if a user explicitly sets a timestamp on the Put
   */
  public void testPutServerClientTimestamps() throws DoNotRetryIOException {
    CheckAndMutateUtil.RequestBuilder underTest = createRequestBuilder();

    long timestamp = (long) (Math.random() * 10000000000L);
    ConditionalRowMutation conditionalRowMutation = underTest
        .qualifier(qual)
        .ifMatches(CompareOp.EQUAL, checkValue)
        .withPut(new Put(rowKey).addColumn(family, qual, timestamp, newValue))
        .build();

    CheckAndMutateRowRequest result = conditionalRowMutation.toProto(REQUEST_CONTEXT);
    Assert.assertEquals(1, result.getTrueMutationsCount());

    Mutation.SetCell setCell = result.getTrueMutations(0).getSetCell();
    Assert.assertEquals(TimeUnit.MILLISECONDS.toMicros(timestamp), setCell.getTimestampMicros());
  }

  @Test
  /**
   * Tests that a CheckAndMutate with a {@link RowMutations} with a {@link Put} which ensures that
   * the conversion to a {@link ConditionalRowMutation} sets a server-side timeatamp (-1) on the
   * {@link com.google.cloud.bigtable.data.v2.models.Mutation}.
   */
  public void testRowMutationServerSideTimestamps() throws IOException {
    CheckAndMutateUtil.RequestBuilder underTest = createRequestBuilder();

    RowMutations rowMutations = new RowMutations(rowKey);
    rowMutations.add(PUT);
    ConditionalRowMutation conditionalRowMutation = underTest
        .qualifier(qual)
        .ifMatches(CompareOp.EQUAL, checkValue)
        .withMutations(rowMutations)
        .build();

    CheckAndMutateRowRequest result = conditionalRowMutation.toProto(REQUEST_CONTEXT);
    Assert.assertEquals(1, result.getTrueMutationsCount());

    Mutation.SetCell setCell = result.getTrueMutations(0).getSetCell();
    Assert.assertEquals(-1, setCell.getTimestampMicros());
  }

  @Test
  /**
   * Test to make sure that {@link CheckAndMutateUtil.RequestBuilder#ifNotExists()} works correctly
   */
  public void testIfNotExists() throws DoNotRetryIOException {
    CheckAndMutateUtil.RequestBuilder underTest = createRequestBuilder();

    ConditionalRowMutation conditionalRowMutation = underTest
        .qualifier(qual)
        .ifNotExists()
        .withPut(PUT)
        .build();

    CheckAndMutateRowRequest result = conditionalRowMutation.toProto(REQUEST_CONTEXT);
    Assert.assertEquals(1, result.getFalseMutationsCount());
    checkPutMutation(result.getFalseMutations(0));

    RowFilter expected = FILTERS.chain()
        .filter(FAMILY_AND_QUAL_FILTER)
        .filter(FILTERS.limit().cellsPerColumn(1))
        .toProto();
    Assert.assertEquals(expected, result.getPredicateFilter());
  }

  @Test
  /**
   * Test to make sure that {@link CheckAndMutateUtil.RequestBuilder#ifMatches(CompareOp, byte[])}
   * wtih {@link CompareOp#NOT_EQUAL} and a null value works as
   * expected.
   */
  public void testNotEqualsNull() throws DoNotRetryIOException {
    CheckAndMutateUtil.RequestBuilder underTest = createRequestBuilder();

    ConditionalRowMutation conditionalRowMutation = underTest
        .qualifier(qual)
        .ifMatches(CompareOp.NOT_EQUAL, null)
        .withPut(PUT)
        .build();

    CheckAndMutateRowRequest result = conditionalRowMutation.toProto(REQUEST_CONTEXT);
    Assert.assertEquals(1, result.getTrueMutationsCount());

    RowFilter expected = FILTERS.chain()
        .filter(FAMILY_AND_QUAL_FILTER)
        .filter(FILTERS.limit().cellsPerColumn(1))
        .toProto();

    checkPutMutation(result.getTrueMutations(0));
    Assert.assertEquals(expected, result.getPredicateFilter());
  }

  @Test
  /**
   * Test to make sure that {@link CheckAndMutateUtil.RequestBuilder#ifMatches(CompareOp, byte[])}
   * wtih {@link CompareOp#NOT_EQUAL} and a null value works as
   * expected.
   */
  public void testCompareOpsOtherThanNotEqualsNull() throws DoNotRetryIOException {
    CheckAndMutateUtil.RequestBuilder underTest = createRequestBuilder();

    List<CompareOp> otherOps = new ArrayList<>(Arrays.asList(CompareOp.values()));

    otherOps.remove(CompareOp.NOT_EQUAL);
    int index = (int) (Math.random() * otherOps.size());
    ConditionalRowMutation conditionalRowMutation = underTest
        .qualifier(qual)
        .ifMatches(otherOps.get(index), null)
        .withPut(PUT)
        .build();

    CheckAndMutateRowRequest result = conditionalRowMutation.toProto(REQUEST_CONTEXT);
    Assert.assertEquals(1, result.getFalseMutationsCount());

    RowFilter expected = FILTERS.chain()
        .filter(FAMILY_AND_QUAL_FILTER)
        .filter(FILTERS.limit().cellsPerColumn(1))
        .toProto();

    checkPutMutation(result.getFalseMutations(0));
    Assert.assertEquals(expected, result.getPredicateFilter());
  }
}
