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
package com.google.cloud.bigtable.mirroring.hbase1_x.asyncwrappers;

import static com.google.common.truth.Truth.assertThat;
import static org.junit.Assert.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.google.cloud.bigtable.mirroring.hbase1_x.MirroringResultScanner;
import com.google.cloud.bigtable.mirroring.hbase1_x.utils.flowcontrol.FlowController;
import com.google.cloud.bigtable.mirroring.hbase1_x.verification.VerificationContinuationFactory;
import com.google.common.util.concurrent.SettableFuture;
import java.io.IOException;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.junit.Test;
import org.junit.function.ThrowingRunnable;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.mockito.Mock;

@RunWith(JUnit4.class)
public class TestMirroringResultScanner {
  @Mock FlowController flowController;

  @Test
  public void testScannerCloseWhenFirstCloseThrows() throws IOException {
    ResultScanner primaryScannerMock = mock(ResultScanner.class);

    VerificationContinuationFactory continuationFactoryMock =
        mock(VerificationContinuationFactory.class);

    AsyncResultScannerWrapper secondaryScannerWrapperMock = mock(AsyncResultScannerWrapper.class);
    AsyncTableWrapper secondaryAsyncTableWrapperMock = mock(AsyncTableWrapper.class);
    when(secondaryAsyncTableWrapperMock.getScanner(any(Scan.class)))
        .thenReturn(secondaryScannerWrapperMock);

    final ResultScanner mirroringScanner =
        new MirroringResultScanner(
            new Scan(),
            primaryScannerMock,
            secondaryAsyncTableWrapperMock,
            continuationFactoryMock,
            flowController);

    doThrow(new RuntimeException("first")).when(primaryScannerMock).close();

    Exception thrown =
        assertThrows(
            RuntimeException.class,
            new ThrowingRunnable() {
              @Override
              public void run() {
                mirroringScanner.close();
              }
            });

    verify(primaryScannerMock, times(1)).close();
    verify(secondaryScannerWrapperMock, times(1)).asyncClose();
    assertThat(thrown).hasMessageThat().contains("first");
  }

  @Test
  public void testScannerCloseWhenSecondCloseThrows() throws IOException {
    ResultScanner primaryScannerMock = mock(ResultScanner.class);

    VerificationContinuationFactory continuationFactoryMock =
        mock(VerificationContinuationFactory.class);

    AsyncResultScannerWrapper secondaryScannerWrapperMock = mock(AsyncResultScannerWrapper.class);
    AsyncTableWrapper secondaryAsyncTableWrapperMock = mock(AsyncTableWrapper.class);
    when(secondaryAsyncTableWrapperMock.getScanner(any(Scan.class)))
        .thenReturn(secondaryScannerWrapperMock);

    final ResultScanner mirroringScanner =
        new MirroringResultScanner(
            new Scan(),
            primaryScannerMock,
            secondaryAsyncTableWrapperMock,
            continuationFactoryMock,
            flowController);

    doThrow(new RuntimeException("second")).when(secondaryScannerWrapperMock).asyncClose();

    Exception thrown =
        assertThrows(
            RuntimeException.class,
            new ThrowingRunnable() {
              @Override
              public void run() {
                mirroringScanner.close();
              }
            });

    verify(primaryScannerMock, times(1)).close();
    verify(secondaryScannerWrapperMock, times(1)).asyncClose();
    assertThat(thrown).hasMessageThat().contains("second");
  }

  @Test
  public void testScannerCloseWhenBothCloseThrow() throws IOException {
    ResultScanner primaryScannerMock = mock(ResultScanner.class);

    VerificationContinuationFactory continuationFactoryMock =
        mock(VerificationContinuationFactory.class);

    AsyncResultScannerWrapper secondaryScannerWrapperMock = mock(AsyncResultScannerWrapper.class);
    AsyncTableWrapper secondaryAsyncTableWrapperMock = mock(AsyncTableWrapper.class);
    when(secondaryAsyncTableWrapperMock.getScanner(any(Scan.class)))
        .thenReturn(secondaryScannerWrapperMock);

    final ResultScanner mirroringScanner =
        new MirroringResultScanner(
            new Scan(),
            primaryScannerMock,
            secondaryAsyncTableWrapperMock,
            continuationFactoryMock,
            flowController);

    doThrow(new RuntimeException("first")).when(primaryScannerMock).close();
    doThrow(new RuntimeException("second")).when(secondaryScannerWrapperMock).asyncClose();

    RuntimeException thrown =
        assertThrows(
            RuntimeException.class,
            new ThrowingRunnable() {
              @Override
              public void run() {
                mirroringScanner.close();
              }
            });

    verify(primaryScannerMock, times(1)).close();
    verify(secondaryScannerWrapperMock, times(1)).asyncClose();
    assertThat(thrown).hasMessageThat().contains("first");
    assertThat(thrown.getSuppressed()).hasLength(1);
    assertThat(thrown.getSuppressed()[0]).hasMessageThat().contains("second");
  }

  @Test
  public void testMultipleCloseCallsCloseScannersOnlyOnce() throws IOException {
    ResultScanner primaryScannerMock = mock(ResultScanner.class);

    VerificationContinuationFactory continuationFactoryMock =
        mock(VerificationContinuationFactory.class);
    AsyncResultScannerWrapper secondaryScannerWrapperMock = mock(AsyncResultScannerWrapper.class);
    SettableFuture<Void> closedFuture = SettableFuture.create();
    closedFuture.set(null);
    when(secondaryScannerWrapperMock.asyncClose()).thenReturn(closedFuture);

    AsyncTableWrapper secondaryAsyncTableWrapperMock = mock(AsyncTableWrapper.class);
    when(secondaryAsyncTableWrapperMock.getScanner(any(Scan.class)))
        .thenReturn(secondaryScannerWrapperMock);

    final ResultScanner mirroringScanner =
        new MirroringResultScanner(
            new Scan(),
            primaryScannerMock,
            secondaryAsyncTableWrapperMock,
            continuationFactoryMock,
            flowController);

    mirroringScanner.close();
    mirroringScanner.close();
    verify(primaryScannerMock, times(1)).close();
    verify(secondaryScannerWrapperMock, times(1)).asyncClose();
  }
}
