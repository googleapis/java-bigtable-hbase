/*
 * Copyright 2021 Google LLC
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
package com.google.cloud.bigtable.mirroring.hbase1_x.bufferedmutator;

import static com.google.cloud.bigtable.mirroring.hbase1_x.TestHelpers.setupFlowControllerMock;
import static com.google.cloud.bigtable.mirroring.hbase1_x.utils.MirroringConfigurationHelper.MIRRORING_BUFFERED_MUTATOR_BYTES_TO_FLUSH;
import static com.google.cloud.bigtable.mirroring.hbase1_x.utils.MirroringConfigurationHelper.MIRRORING_PRIMARY_CONFIG_PREFIX_KEY;
import static com.google.cloud.bigtable.mirroring.hbase1_x.utils.MirroringConfigurationHelper.MIRRORING_PRIMARY_CONNECTION_CLASS_KEY;
import static com.google.cloud.bigtable.mirroring.hbase1_x.utils.MirroringConfigurationHelper.MIRRORING_SECONDARY_CONFIG_PREFIX_KEY;
import static com.google.cloud.bigtable.mirroring.hbase1_x.utils.MirroringConfigurationHelper.MIRRORING_SECONDARY_CONNECTION_CLASS_KEY;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;

import com.google.cloud.bigtable.mirroring.hbase1_x.MirroringConfiguration;
import com.google.cloud.bigtable.mirroring.hbase1_x.MirroringConnection;
import com.google.cloud.bigtable.mirroring.hbase1_x.TestConnection;
import com.google.cloud.bigtable.mirroring.hbase1_x.utils.SecondaryWriteErrorConsumerWithMetrics;
import com.google.cloud.bigtable.mirroring.hbase1_x.utils.flowcontrol.FlowController;
import com.google.cloud.bigtable.mirroring.hbase1_x.utils.flowcontrol.FlowController.ResourceReservation;
import com.google.common.util.concurrent.SettableFuture;
import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.BufferedMutator;
import org.apache.hadoop.hbase.client.BufferedMutatorParams;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Mutation;
import org.apache.hadoop.hbase.client.RetriesExhaustedWithDetailsException;
import org.apache.hadoop.hbase.client.Row;
import org.mockito.ArgumentCaptor;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

public class MirroringBufferedMutatorCommon {
  public final Connection primaryConnection = mock(Connection.class);
  public final Connection secondaryConnection = mock(Connection.class);
  public final FlowController flowController = mock(FlowController.class);
  public final SecondaryWriteErrorConsumerWithMetrics secondaryWriteErrorConsumerWithMetrics =
      mock(SecondaryWriteErrorConsumerWithMetrics.class);

  public final BufferedMutatorParams bufferedMutatorParams =
      new BufferedMutatorParams(TableName.valueOf("test1"));

  public final BufferedMutator primaryBufferedMutator = mock(BufferedMutator.class);
  public final BufferedMutator secondaryBufferedMutator = mock(BufferedMutator.class);

  public final ArgumentCaptor<BufferedMutatorParams> primaryBufferedMutatorParamsCaptor;
  public final ArgumentCaptor<BufferedMutatorParams> secondaryBufferedMutatorParamsCaptor;

  public final ResourceReservation resourceReservation;

  public final Mutation mutation1 = new Delete("key1".getBytes());
  public final Mutation mutation2 = new Delete("key2".getBytes());
  public final Mutation mutation3 = new Delete("key3".getBytes());
  public final Mutation mutation4 = new Delete("key4".getBytes());
  public final long mutationSize = mutation1.heapSize();

  public MirroringBufferedMutatorCommon() {
    this.primaryBufferedMutatorParamsCaptor = ArgumentCaptor.forClass(BufferedMutatorParams.class);
    try {
      doReturn(primaryBufferedMutator)
          .when(primaryConnection)
          .getBufferedMutator(primaryBufferedMutatorParamsCaptor.capture());
    } catch (IOException e) {
      throw new RuntimeException(e);
    }

    this.secondaryBufferedMutatorParamsCaptor =
        ArgumentCaptor.forClass(BufferedMutatorParams.class);
    try {
      doReturn(secondaryBufferedMutator)
          .when(secondaryConnection)
          .getBufferedMutator(secondaryBufferedMutatorParamsCaptor.capture());
    } catch (IOException e) {
      throw new RuntimeException(e);
    }

    resourceReservation = setupFlowControllerMock(flowController);
  }

  public static Answer<Void> mutateWithErrors(
      final ArgumentCaptor<BufferedMutatorParams> argumentCaptor,
      final BufferedMutator bufferedMutator,
      final Mutation... failingMutations) {
    return new Answer<Void>() {
      @Override
      public Void answer(InvocationOnMock invocationOnMock) throws Throwable {
        List<Mutation> failingMutationsList = Arrays.asList(failingMutations);
        List<? extends Mutation> argument = invocationOnMock.getArgument(0);
        for (Mutation m : argument) {
          if (failingMutationsList.contains(m)) {
            callErrorHandler(m, argumentCaptor, bufferedMutator);
          }
        }
        return null;
      }
    };
  }

  public static Answer<Void> flushWithErrors(
      final ArgumentCaptor<BufferedMutatorParams> argumentCaptor,
      final BufferedMutator bufferedMutator,
      final Mutation... failingMutations) {
    return new Answer<Void>() {
      @Override
      public Void answer(InvocationOnMock invocationOnMock) throws Throwable {
        for (Mutation m : failingMutations) {
          callErrorHandler(m, argumentCaptor, bufferedMutator);
        }
        return null;
      }
    };
  }

  private static void callErrorHandler(
      Mutation m,
      ArgumentCaptor<BufferedMutatorParams> argumentCaptor,
      BufferedMutator bufferedMutator)
      throws RetriesExhaustedWithDetailsException {
    argumentCaptor
        .getValue()
        .getListener()
        .onException(
            new RetriesExhaustedWithDetailsException(
                Arrays.asList(new Throwable[] {new IOException()}),
                Arrays.asList(new Row[] {m}),
                Arrays.asList("invalid.example:1234")),
            bufferedMutator);
  }

  public static MirroringConfiguration makeConfigurationWithFlushThreshold(long flushThreshold) {
    Configuration mirroringConfig = new Configuration();
    mirroringConfig.set(
        "hbase.client.connection.impl", MirroringConnection.class.getCanonicalName());

    mirroringConfig.set(
        MIRRORING_PRIMARY_CONNECTION_CLASS_KEY, TestConnection.class.getCanonicalName());
    mirroringConfig.set(MIRRORING_PRIMARY_CONFIG_PREFIX_KEY, "prefix1");
    mirroringConfig.set(
        MIRRORING_SECONDARY_CONNECTION_CLASS_KEY, TestConnection.class.getCanonicalName());
    mirroringConfig.set(MIRRORING_SECONDARY_CONFIG_PREFIX_KEY, "prefix2");
    mirroringConfig.set(MIRRORING_BUFFERED_MUTATOR_BYTES_TO_FLUSH, String.valueOf(flushThreshold));

    return new MirroringConfiguration(mirroringConfig);
  }

  public static Answer<Void> blockedFlushes(
      final AtomicInteger ongoingFlushes,
      final SettableFuture<Void> allFlushesStarted,
      final SettableFuture<Void> endFlush,
      final int expectedNumberOfFlushes) {
    return new Answer<Void>() {
      @Override
      public Void answer(InvocationOnMock invocationOnMock) throws Throwable {
        if (ongoingFlushes.incrementAndGet() == expectedNumberOfFlushes) {
          allFlushesStarted.set(null);
        }
        endFlush.get();
        return null;
      }
    };
  }
}
