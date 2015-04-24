package com.google.cloud.hadoop.hbase;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import com.google.bigtable.v1.BigtableServiceGrpc;
import com.google.bigtable.v1.CheckAndMutateRowRequest;
import com.google.bigtable.v1.MutateRowRequest;
import com.google.bigtable.v1.Mutation;
import com.google.bigtable.v1.Mutation.SetCell;
import com.google.common.base.Predicate;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

import io.grpc.MethodDescriptor;

import java.util.Map;

@RunWith(JUnit4.class)
public class BigtableChannelsTest {

  @SuppressWarnings("unchecked")
  @Test
  public void createMethodRetryMap() {
    Map<MethodDescriptor<?, ?>, Predicate<?>> map = BigtableChannels.createMethodRetryMap();
    for (MethodDescriptor<?, ?> method: BigtableServiceGrpc.CONFIG.methods()) {
      if (method == BigtableServiceGrpc.CONFIG.mutateRow) {
        assertMutateRowPredicate((Predicate<MutateRowRequest>) map.get(method));
      } else if (method == BigtableServiceGrpc.CONFIG.checkAndMutateRow) {
        assertCheckAndMutateRowPredicate((Predicate<CheckAndMutateRowRequest>) map.get(method));
      } else {
        assertNull(map.get(method));
      }
    }
  }

  private static void assertMutateRowPredicate(Predicate<MutateRowRequest> predicate) {
    assertFalse(predicate.apply(null));

    MutateRowRequest.Builder request = MutateRowRequest.newBuilder();
    assertTrue(predicate.apply(request.build()));

    request.addMutations(
        Mutation.newBuilder().setSetCell(SetCell.newBuilder().setTimestampMicros(-1)));
    assertFalse(predicate.apply(request.build()));
  }

  private static void assertCheckAndMutateRowPredicate(
      Predicate<CheckAndMutateRowRequest> predicate) {
    assertFalse(predicate.apply(null));

    CheckAndMutateRowRequest.Builder request = CheckAndMutateRowRequest.newBuilder();
    assertTrue(predicate.apply(request.build()));

    request.addTrueMutations(
        Mutation.newBuilder().setSetCell(SetCell.newBuilder().setTimestampMicros(-1)));
    assertFalse(predicate.apply(request.build()));

    request.clearTrueMutations();
    request.addFalseMutations(
        Mutation.newBuilder().setSetCell(SetCell.newBuilder().setTimestampMicros(-1)));
    assertFalse(predicate.apply(request.build()));
  }
}
