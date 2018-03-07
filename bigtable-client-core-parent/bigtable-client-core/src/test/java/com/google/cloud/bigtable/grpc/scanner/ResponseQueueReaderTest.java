/*
 * Copyright 2017 Google Inc. All Rights Reserved.
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

import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;
import static org.mockito.Matchers.anyInt;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import java.io.IOException;
import java.util.concurrent.TimeUnit;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import com.google.cloud.bigtable.grpc.BigtableSessionSharedThreadPools;
import com.google.cloud.bigtable.grpc.io.IOExceptionWithStatus;
import com.google.protobuf.ByteString;

import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import io.grpc.stub.ClientCallStreamObserver;

@RunWith(JUnit4.class)
@SuppressWarnings({"rawtypes", "unchecked"})
public class ResponseQueueReaderTest {

  @Mock
  private ClientCallStreamObserver mockClientCallStreamObserver;

  @Rule
  public ExpectedException expectedException = ExpectedException.none();

  private ResponseQueueReader underTest;

  @Before
  public void setup() {
    MockitoAnnotations.initMocks(this);
    underTest = new ResponseQueueReader(10000);
    underTest.beforeStart(mockClientCallStreamObserver);
  }

  @Test
  public void testNone() throws IOException {
    underTest.onCompleted();
    assertNull(underTest.getNextMergedRow());
  }

  @Test
  public void testSinglePostComplete() throws IOException {
    FlatRow row = new FlatRow(ByteString.EMPTY, null);
    underTest.onNext(row);
    underTest.onReadRowsResponseComplete();
    underTest.onCompleted();
    assertSame(row, underTest.getNextMergedRow());
    verify(mockClientCallStreamObserver, times(0)).request(anyInt());
  }

  @Test
  public void testSinglePrecomplete() throws IOException {
    FlatRow row = new FlatRow(ByteString.EMPTY, null);
    underTest.onNext(row);
    underTest.onReadRowsResponseComplete();
    assertSame(row, underTest.getNextMergedRow());
    // getNextMergedRow() will block until either a result is present or the operation is complete.
    BigtableSessionSharedThreadPools.getInstance().getRetryExecutor().schedule(new Runnable() {
      @Override
      public void run() {
        underTest.onCompleted();
      }
    }, 50, TimeUnit.MILLISECONDS);
    assertNull(underTest.getNextMergedRow());
    verify(mockClientCallStreamObserver, times(1)).request(eq(1));
  }

  @Test
  public void testException() throws IOException {
    StatusRuntimeException exception = Status.DEADLINE_EXCEEDED.asRuntimeException();
    expectedException.expect(IOExceptionWithStatus.class);
    underTest.onError(exception);
    underTest.getNextMergedRow();
  }
}
