package com.google.cloud.bigtable.hbase.replication;

import static com.google.cloud.bigtable.hbase.replication.utils.TestUtils.CF1;
import static com.google.cloud.bigtable.hbase.replication.utils.TestUtils.CF2;
import static com.google.cloud.bigtable.hbase.replication.utils.TestUtils.COL_QUALIFIER;
import static com.google.cloud.bigtable.hbase.replication.utils.TestUtils.ROW_KEY;
import static com.google.cloud.bigtable.hbase.replication.utils.TestUtils.TIMESTAMP;
import static com.google.cloud.bigtable.hbase.replication.utils.TestUtils.VALUE;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import com.google.cloud.bigtable.hbase.replication.CloudBigtableReplicationTask.DeleteMutationBuilder;
import com.google.cloud.bigtable.hbase.replication.CloudBigtableReplicationTask.MutationBuilder;
import com.google.cloud.bigtable.hbase.replication.CloudBigtableReplicationTask.MutationBuilderFactory;
import com.google.cloud.bigtable.hbase.replication.CloudBigtableReplicationTask.PutMutationBuilder;
import java.io.IOException;

import com.google.cloud.bigtable.hbase.replication.utils.TestUtils;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.RowMutations;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
public class MutationBuilderTest {

  @Test
  public void testPutBuilderAcceptsOnlyPuts() {
    PutMutationBuilder putMutationBuilder = new PutMutationBuilder(ROW_KEY);
    assertTrue(putMutationBuilder.canAcceptMutation(
        new KeyValue(ROW_KEY, CF1, COL_QUALIFIER, TIMESTAMP, KeyValue.Type.Put, VALUE)));
    assertFalse(putMutationBuilder.canAcceptMutation(
        new KeyValue(ROW_KEY, CF1, COL_QUALIFIER, TIMESTAMP, KeyValue.Type.Delete)));
    assertFalse(putMutationBuilder.canAcceptMutation(
        new KeyValue(ROW_KEY, CF1, COL_QUALIFIER, TIMESTAMP, KeyValue.Type.DeleteFamily)));
    assertFalse(putMutationBuilder.canAcceptMutation(
        new KeyValue(ROW_KEY, CF1, COL_QUALIFIER, TIMESTAMP, KeyValue.Type.DeleteFamilyVersion)));
    assertFalse(putMutationBuilder.canAcceptMutation(
        new KeyValue(ROW_KEY, CF1, COL_QUALIFIER, TIMESTAMP, KeyValue.Type.DeleteColumn)));
    assertFalse(putMutationBuilder.canAcceptMutation(
        new KeyValue(ROW_KEY, CF1, COL_QUALIFIER, TIMESTAMP, KeyValue.Type.Maximum)));
    assertFalse(putMutationBuilder.canAcceptMutation(
        new KeyValue(ROW_KEY, CF1, COL_QUALIFIER, TIMESTAMP, KeyValue.Type.Minimum)));
  }

  @Test
  public void testPutBuild() throws IOException {
    PutMutationBuilder mutationBuilder = new PutMutationBuilder(ROW_KEY);
    mutationBuilder.addMutation(
        new KeyValue(ROW_KEY, CF1, COL_QUALIFIER, TIMESTAMP, KeyValue.Type.Put, VALUE));
    mutationBuilder.addMutation(
        new KeyValue(ROW_KEY, CF2, COL_QUALIFIER, TIMESTAMP + 1, KeyValue.Type.Put, VALUE));

    Put expectedPut = new Put(ROW_KEY);
    expectedPut.add(
        new KeyValue(ROW_KEY, CF1, COL_QUALIFIER, TIMESTAMP, KeyValue.Type.Put, VALUE));
    expectedPut.add(
        new KeyValue(ROW_KEY, CF2, COL_QUALIFIER, TIMESTAMP + 1, KeyValue.Type.Put, VALUE));

    RowMutations rowMutations = new RowMutations(ROW_KEY);

    // call the mutationBuilder.build()
    mutationBuilder.buildAndUpdateRowMutations(rowMutations);
    TestUtils.assertEquals(expectedPut, rowMutations.getMutations().get(0));
  }

  @Test(expected = IllegalStateException.class)
  public void testPutBuilderDoesNotAddAfterBuild() throws IOException {
    PutMutationBuilder mutationBuilder = new PutMutationBuilder(ROW_KEY);
    mutationBuilder.addMutation(
        new KeyValue(ROW_KEY, CF1, COL_QUALIFIER, TIMESTAMP, KeyValue.Type.Put, VALUE));
    mutationBuilder.buildAndUpdateRowMutations(new RowMutations(ROW_KEY));
    mutationBuilder.addMutation(
        new KeyValue(ROW_KEY, CF1, COL_QUALIFIER, TIMESTAMP, KeyValue.Type.Put, VALUE));
  }

  @Test(expected = IllegalStateException.class)
  public void testPutBuilderDoesNotAcceptAfterBuild() throws IOException {
    PutMutationBuilder mutationBuilder = new PutMutationBuilder(ROW_KEY);
    mutationBuilder.addMutation(
        new KeyValue(ROW_KEY, CF1, COL_QUALIFIER, TIMESTAMP, KeyValue.Type.Put, VALUE));
    mutationBuilder.buildAndUpdateRowMutations(new RowMutations(ROW_KEY));
    mutationBuilder.canAcceptMutation(
        new KeyValue(ROW_KEY, CF1, COL_QUALIFIER, TIMESTAMP, KeyValue.Type.Put, VALUE));
  }


  // =================== DELETE ====================================
  @Test
  public void testDeleteAcceptsDelete() {
    DeleteMutationBuilder deleteMutationBuilder = new DeleteMutationBuilder(ROW_KEY);
    assertTrue(deleteMutationBuilder.canAcceptMutation(
        new KeyValue(ROW_KEY, CF1, COL_QUALIFIER, TIMESTAMP, KeyValue.Type.Delete, VALUE)));
    assertTrue(deleteMutationBuilder.canAcceptMutation(
        new KeyValue(ROW_KEY, CF1, COL_QUALIFIER, TIMESTAMP, KeyValue.Type.DeleteFamily, VALUE)));
    assertTrue(deleteMutationBuilder.canAcceptMutation(
        new KeyValue(ROW_KEY, CF1, COL_QUALIFIER, TIMESTAMP, KeyValue.Type.DeleteFamilyVersion,
            VALUE)));
    assertTrue(deleteMutationBuilder.canAcceptMutation(
        new KeyValue(ROW_KEY, CF1, COL_QUALIFIER, TIMESTAMP, KeyValue.Type.DeleteColumn, VALUE)));

    assertFalse(deleteMutationBuilder.canAcceptMutation(
        new KeyValue(ROW_KEY, CF1, COL_QUALIFIER, TIMESTAMP, KeyValue.Type.Put)));
    assertFalse(deleteMutationBuilder.canAcceptMutation(
        new KeyValue(ROW_KEY, CF1, COL_QUALIFIER, TIMESTAMP, KeyValue.Type.Maximum)));
    assertFalse(deleteMutationBuilder.canAcceptMutation(
        new KeyValue(ROW_KEY, CF1, COL_QUALIFIER, TIMESTAMP, KeyValue.Type.Minimum)));
  }

  @Test
  public void testDeleteBuild() throws IOException {
    DeleteMutationBuilder mutationBuilder = new DeleteMutationBuilder(ROW_KEY);
    mutationBuilder.addMutation(
        new KeyValue(ROW_KEY, CF1, COL_QUALIFIER, TIMESTAMP, KeyValue.Type.Delete, VALUE));
    mutationBuilder.addMutation(
        new KeyValue(ROW_KEY, CF2, COL_QUALIFIER, TIMESTAMP + 1, KeyValue.Type.DeleteFamily,
            VALUE));

    Delete expectedPut = new Delete(ROW_KEY);
    expectedPut.addDeleteMarker(
        new KeyValue(ROW_KEY, CF1, COL_QUALIFIER, TIMESTAMP, KeyValue.Type.Delete, VALUE));
    expectedPut.addDeleteMarker(
        new KeyValue(ROW_KEY, CF2, COL_QUALIFIER, TIMESTAMP + 1, KeyValue.Type.DeleteFamily,
            VALUE));

    RowMutations rowMutations = new RowMutations(ROW_KEY);

    // call the mutationBuilder.build()
    mutationBuilder.buildAndUpdateRowMutations(rowMutations);
    TestUtils.assertEquals(expectedPut, rowMutations.getMutations().get(0));
  }

  @Test
  public void testEmptyDeleteBuildDoesNothing() throws IOException {
    DeleteMutationBuilder mutationBuilder = new DeleteMutationBuilder(ROW_KEY);

    RowMutations rowMutations = new RowMutations(ROW_KEY);

    // call the mutationBuilder.build()
    mutationBuilder.buildAndUpdateRowMutations(rowMutations);
    assertTrue(rowMutations.getMutations().isEmpty());
  }

  @Test(expected = IllegalStateException.class)
  public void testDeleteBuilderDoesNotAddAfterBuild() throws IOException {
    DeleteMutationBuilder mutationBuilder = new DeleteMutationBuilder(ROW_KEY);
    mutationBuilder.addMutation(
        new KeyValue(ROW_KEY, CF1, COL_QUALIFIER, TIMESTAMP, KeyValue.Type.Delete));
    mutationBuilder.buildAndUpdateRowMutations(new RowMutations(ROW_KEY));
    mutationBuilder.addMutation(
        new KeyValue(ROW_KEY, CF1, COL_QUALIFIER, TIMESTAMP, KeyValue.Type.Delete));
  }

  @Test(expected = IllegalStateException.class)
  public void testDeleteBuilderDoesNotAcceptAfterBuild() throws IOException {
    DeleteMutationBuilder mutationBuilder = new DeleteMutationBuilder(ROW_KEY);
    mutationBuilder.addMutation(
        new KeyValue(ROW_KEY, CF1, COL_QUALIFIER, TIMESTAMP, KeyValue.Type.Delete));
    mutationBuilder.buildAndUpdateRowMutations(new RowMutations(ROW_KEY));
    mutationBuilder.canAcceptMutation(
        new KeyValue(ROW_KEY, CF1, COL_QUALIFIER, TIMESTAMP, KeyValue.Type.Delete));
  }

  @Test
  public void testMutationBuilderFactory() {
    Cell put = new KeyValue(ROW_KEY, CF1, COL_QUALIFIER, TIMESTAMP, KeyValue.Type.Put);
    MutationBuilder builder = MutationBuilderFactory.getMutationBuilder(put);

    assertTrue(builder instanceof PutMutationBuilder);

    Cell delete = new KeyValue(ROW_KEY, CF1, COL_QUALIFIER, TIMESTAMP, KeyValue.Type.Delete);
    builder = MutationBuilderFactory.getMutationBuilder(delete);
    assertTrue(builder instanceof DeleteMutationBuilder);
  }

  @Test(expected = UnsupportedOperationException.class)
  public void testMutationBuilderFactoryUnsupportedType() {
    Cell unknownType = new KeyValue(ROW_KEY, CF1, COL_QUALIFIER, TIMESTAMP, KeyValue.Type.Minimum);
    MutationBuilder builder = MutationBuilderFactory.getMutationBuilder(unknownType);
  }

}