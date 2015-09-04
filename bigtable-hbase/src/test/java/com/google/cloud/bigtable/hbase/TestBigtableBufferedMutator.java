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
package com.google.cloud.bigtable.hbase;

import static org.mockito.Matchers.any;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import io.grpc.stub.StreamObserver;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Append;
import org.apache.hadoop.hbase.client.BufferedMutator;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Increment;
import org.apache.hadoop.hbase.client.Mutation;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.RetriesExhaustedWithDetailsException;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

import com.google.bigtable.v1.MutateRowRequest;
import com.google.bigtable.v1.ReadModifyWriteRowRequest;
import com.google.cloud.bigtable.config.BigtableOptions;
import com.google.cloud.bigtable.grpc.async.BigtableAsyncExecutor;

/**
 * Tests for {@link BigtableBufferedMutator}
 */
@RunWith(JUnit4.class)
public class TestBigtableBufferedMutator {

  @Mock
  BigtableAsyncExecutor asyncExecutor;

  private BigtableBufferedMutator underTest;

  @Before
  public void setUp() {
    MockitoAnnotations.initMocks(this);
    BufferedMutator.ExceptionListener listener = new BufferedMutator.ExceptionListener() {
      @Override
      public void onException(RetriesExhaustedWithDetailsException exception,
          BufferedMutator mutator) throws RetriesExhaustedWithDetailsException {
        throw exception;
      }
    };
    BigtableOptions options =
        new BigtableOptions.Builder().setProjectId("project").setZoneId("zone")
            .setClusterId("cluster").setUserAgent("userAgent").build();
    underTest =
        new BigtableBufferedMutator(asyncExecutor, listener, new Configuration(),
            TableName.valueOf("TABLE"), options);
  }

  @Test
  public void testPut() throws IOException {
    underTest.mutate(new Put(new byte[1]).addColumn(new byte[1], new byte[1], new byte[1]));
    verify(this.asyncExecutor, times(1)).mutateRowAsync(any(MutateRowRequest.class));
  }


  @Test
  public void testDelete() throws IOException {
    underTest.mutate(new Delete(new byte[1]));
    verify(this.asyncExecutor, times(1)).mutateRowAsync(any(MutateRowRequest.class));
  }

  @Test
  public void testApppend() throws IOException {
    underTest.mutate(new Append(new byte[1]));
    verify(this.asyncExecutor, times(1)).readModifyWriteRowAsync(
      any(ReadModifyWriteRowRequest.class));
  }

  @Test
  public void testIncrement() throws IOException {
    underTest.mutate(new Increment(new byte[1]));
    verify(this.asyncExecutor, times(1)).readModifyWriteRowAsync(
      any(ReadModifyWriteRowRequest.class));
  }

  @Test
  public void testUnknwon() throws IOException {
    underTest.mutate(new Mutation(){});
    Assert.assertEquals(1, underTest.globalExceptions.size());
  }

  @Test
  public void testInvalidPut() throws Exception {
    Mockito.doAnswer(new Answer<Void>() {

      @Override
      public Void answer(InvocationOnMock invocation) throws Throwable {
        @SuppressWarnings("rawtypes")
        StreamObserver observer = (StreamObserver) invocation.getArguments()[1];
        observer.onError(new RuntimeException());
        return null;
      }
    }).when(this.asyncExecutor).mutateRowAsync(any(MutateRowRequest.class));
    underTest.mutate(new Delete(new byte[1]));
    Assert.assertEquals(1, underTest.globalExceptions.size());
    try {
      underTest.handleExceptions();
      Assert.fail("expected RetriesExhaustedWithDetailsException");
    } catch (RetriesExhaustedWithDetailsException expected) {
      // Expected
    }
  }
}
