package com.google.cloud.bigtable.hbase.replication;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import com.google.cloud.bigtable.hbase.replication.CloudBigtableReplicationTask.DeleteMutationBuilder;
import com.google.cloud.bigtable.hbase.replication.CloudBigtableReplicationTask.PutMutationBuilder;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.RowMutations;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
public class MutationBuilderTest {

  public static final byte[] ROW_KEY_1 = "some_row".getBytes(StandardCharsets.UTF_8);
  public static final byte[] FAMILY = "cf".getBytes(StandardCharsets.UTF_8);
  public static final byte[] FAMILY_2 = "cf2".getBytes(StandardCharsets.UTF_8);
  public static final byte[] QUALIFIER = "qualifier".getBytes(StandardCharsets.UTF_8);
  public static final long TIMESTAMP = 0;
  public static final byte[] VALUE = "random".getBytes(StandardCharsets.UTF_8);

  @Test
  public void testPutBuiderAcceptsOnlyPuts() {
    PutMutationBuilder putMutationBuilder = new PutMutationBuilder(ROW_KEY_1);
    assertTrue(putMutationBuilder.canAcceptMutation(
        new KeyValue(ROW_KEY_1, FAMILY, QUALIFIER, TIMESTAMP, KeyValue.Type.Put, VALUE)));
    assertFalse(putMutationBuilder.canAcceptMutation(
        new KeyValue(ROW_KEY_1, FAMILY, QUALIFIER, TIMESTAMP, KeyValue.Type.Delete)));
    assertFalse(putMutationBuilder.canAcceptMutation(
        new KeyValue(ROW_KEY_1, FAMILY, QUALIFIER, TIMESTAMP, KeyValue.Type.DeleteFamily)));
    assertFalse(putMutationBuilder.canAcceptMutation(
        new KeyValue(ROW_KEY_1, FAMILY, QUALIFIER, TIMESTAMP, KeyValue.Type.DeleteFamilyVersion)));
    assertFalse(putMutationBuilder.canAcceptMutation(
        new KeyValue(ROW_KEY_1, FAMILY, QUALIFIER, TIMESTAMP, KeyValue.Type.DeleteColumn)));
    assertFalse(putMutationBuilder.canAcceptMutation(
        new KeyValue(ROW_KEY_1, FAMILY, QUALIFIER, TIMESTAMP, KeyValue.Type.Maximum)));
    assertFalse(putMutationBuilder.canAcceptMutation(
        new KeyValue(ROW_KEY_1, FAMILY, QUALIFIER, TIMESTAMP, KeyValue.Type.Minimum)));
  }

  @Test
  public void testPutBuild() throws IOException {
    PutMutationBuilder mutationBuilder = new PutMutationBuilder(ROW_KEY_1);
    mutationBuilder.addMutation(
        new KeyValue(ROW_KEY_1, FAMILY, QUALIFIER, TIMESTAMP, KeyValue.Type.Put, VALUE));
    mutationBuilder.addMutation(
        new KeyValue(ROW_KEY_1, FAMILY_2, QUALIFIER, TIMESTAMP + 1, KeyValue.Type.Put, VALUE));

    Put expectedPut = new Put(ROW_KEY_1);
    expectedPut.add(
        new KeyValue(ROW_KEY_1, FAMILY, QUALIFIER, TIMESTAMP, KeyValue.Type.Put, VALUE));
    expectedPut.add(
        new KeyValue(ROW_KEY_1, FAMILY_2, QUALIFIER, TIMESTAMP + 1, KeyValue.Type.Put, VALUE));

    RowMutations rowMutations = new RowMutations(ROW_KEY_1);

    // call the mutationBuilder.build()
    mutationBuilder.buildAndUpdateRowMutations(rowMutations);
    TestUtils.assertEquals(expectedPut, rowMutations.getMutations().get(0));
  }

  @Test(expected = IllegalStateException.class)
  public void testPutBuilderDoesNotAddAfterBuild() throws IOException {
    PutMutationBuilder mutationBuilder = new PutMutationBuilder(ROW_KEY_1);
    mutationBuilder.addMutation(
        new KeyValue(ROW_KEY_1, FAMILY, QUALIFIER, TIMESTAMP, KeyValue.Type.Put, VALUE));
    mutationBuilder.buildAndUpdateRowMutations(new RowMutations(ROW_KEY_1));
    mutationBuilder.addMutation(
        new KeyValue(ROW_KEY_1, FAMILY, QUALIFIER, TIMESTAMP, KeyValue.Type.Put, VALUE));
  }

  @Test(expected = IllegalStateException.class)
  public void testPutBuilderDoesNotAcceptAfterBuild() throws IOException {
    PutMutationBuilder mutationBuilder = new PutMutationBuilder(ROW_KEY_1);
    mutationBuilder.addMutation(
        new KeyValue(ROW_KEY_1, FAMILY, QUALIFIER, TIMESTAMP, KeyValue.Type.Put, VALUE));
    mutationBuilder.buildAndUpdateRowMutations(new RowMutations(ROW_KEY_1));
    mutationBuilder.canAcceptMutation(
        new KeyValue(ROW_KEY_1, FAMILY, QUALIFIER, TIMESTAMP, KeyValue.Type.Put, VALUE));
  }


  // =================== DELETE ====================================
  @Test
  public void testDeleteAcceptsDelete() {
    DeleteMutationBuilder deleteMutationBuilder = new DeleteMutationBuilder(ROW_KEY_1);
    assertTrue(deleteMutationBuilder.canAcceptMutation(
        new KeyValue(ROW_KEY_1, FAMILY, QUALIFIER, TIMESTAMP, KeyValue.Type.Delete, VALUE)));
    assertTrue(deleteMutationBuilder.canAcceptMutation(
        new KeyValue(ROW_KEY_1, FAMILY, QUALIFIER, TIMESTAMP, KeyValue.Type.DeleteFamily, VALUE)));
    assertTrue(deleteMutationBuilder.canAcceptMutation(
        new KeyValue(ROW_KEY_1, FAMILY, QUALIFIER, TIMESTAMP, KeyValue.Type.DeleteFamilyVersion,
            VALUE)));
    assertTrue(deleteMutationBuilder.canAcceptMutation(
        new KeyValue(ROW_KEY_1, FAMILY, QUALIFIER, TIMESTAMP, KeyValue.Type.DeleteColumn, VALUE)));

    assertFalse(deleteMutationBuilder.canAcceptMutation(
        new KeyValue(ROW_KEY_1, FAMILY, QUALIFIER, TIMESTAMP, KeyValue.Type.Put)));
    assertFalse(deleteMutationBuilder.canAcceptMutation(
        new KeyValue(ROW_KEY_1, FAMILY, QUALIFIER, TIMESTAMP, KeyValue.Type.Maximum)));
    assertFalse(deleteMutationBuilder.canAcceptMutation(
        new KeyValue(ROW_KEY_1, FAMILY, QUALIFIER, TIMESTAMP, KeyValue.Type.Minimum)));
  }

  @Test
  public void testDeleteBuild() throws IOException {
    DeleteMutationBuilder mutationBuilder = new DeleteMutationBuilder(ROW_KEY_1);
    mutationBuilder.addMutation(
        new KeyValue(ROW_KEY_1, FAMILY, QUALIFIER, TIMESTAMP, KeyValue.Type.Delete, VALUE));
    mutationBuilder.addMutation(
        new KeyValue(ROW_KEY_1, FAMILY_2, QUALIFIER, TIMESTAMP + 1, KeyValue.Type.DeleteFamily,
            VALUE));

    Delete expectedPut = new Delete(ROW_KEY_1);
    expectedPut.addDeleteMarker(
        new KeyValue(ROW_KEY_1, FAMILY, QUALIFIER, TIMESTAMP, KeyValue.Type.Delete, VALUE));
    expectedPut.addDeleteMarker(
        new KeyValue(ROW_KEY_1, FAMILY_2, QUALIFIER, TIMESTAMP + 1, KeyValue.Type.DeleteFamily,
            VALUE));

    RowMutations rowMutations = new RowMutations(ROW_KEY_1);

    // call the mutationBuilder.build()
    mutationBuilder.buildAndUpdateRowMutations(rowMutations);
    TestUtils.assertEquals(expectedPut, rowMutations.getMutations().get(0));
  }

  @Test
  public void testEmptyDeleteBuildDoesNothing() throws IOException {
    DeleteMutationBuilder mutationBuilder = new DeleteMutationBuilder(ROW_KEY_1);

    RowMutations rowMutations = new RowMutations(ROW_KEY_1);

    // call the mutationBuilder.build()
    mutationBuilder.buildAndUpdateRowMutations(rowMutations);
    assertTrue(rowMutations.getMutations().isEmpty());
  }

  @Test(expected = IllegalStateException.class)
  public void testDeleteBuilderDoesNotAddAfterBuild() throws IOException {
    DeleteMutationBuilder mutationBuilder = new DeleteMutationBuilder(ROW_KEY_1);
    mutationBuilder.addMutation(
        new KeyValue(ROW_KEY_1, FAMILY, QUALIFIER, TIMESTAMP, KeyValue.Type.Delete));
    mutationBuilder.buildAndUpdateRowMutations(new RowMutations(ROW_KEY_1));
    mutationBuilder.addMutation(
        new KeyValue(ROW_KEY_1, FAMILY, QUALIFIER, TIMESTAMP, KeyValue.Type.Delete));
  }

  @Test(expected = IllegalStateException.class)
  public void testDeleteBuilderDoesNotAcceptAfterBuild() throws IOException {
    DeleteMutationBuilder mutationBuilder = new DeleteMutationBuilder(ROW_KEY_1);
    mutationBuilder.addMutation(
        new KeyValue(ROW_KEY_1, FAMILY, QUALIFIER, TIMESTAMP, KeyValue.Type.Delete));
    mutationBuilder.buildAndUpdateRowMutations(new RowMutations(ROW_KEY_1));
    mutationBuilder.canAcceptMutation(
        new KeyValue(ROW_KEY_1, FAMILY, QUALIFIER, TIMESTAMP, KeyValue.Type.Delete));
  }

}