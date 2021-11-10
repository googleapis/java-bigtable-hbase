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
import com.google.cloud.bigtable.mirroring.hbase1_x.asyncwrappers.AsyncResultScannerWrapper.AsyncScannerVerificationPayload;
import com.google.cloud.bigtable.mirroring.hbase1_x.asyncwrappers.AsyncResultScannerWrapper.ScannerRequestContext;
import com.google.cloud.bigtable.mirroring.hbase1_x.utils.flowcontrol.FlowController;
import com.google.cloud.bigtable.mirroring.hbase1_x.utils.mirroringmetrics.MirroringTracer;
import com.google.cloud.bigtable.mirroring.hbase1_x.verification.VerificationContinuationFactory;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.ListeningExecutorService;
import com.google.common.util.concurrent.MoreExecutors;
import com.google.common.util.concurrent.SettableFuture;
import io.opencensus.trace.Span;
import io.opencensus.trace.Tracing;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.checkerframework.checker.nullness.compatqual.NullableDecl;
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

    final ResultScanner mirroringScanner =
        new MirroringResultScanner(
            new Scan(),
            primaryScannerMock,
            secondaryScannerWrapperMock,
            continuationFactoryMock,
            flowController,
            new MirroringTracer(),
            true);

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

    final ResultScanner mirroringScanner =
        new MirroringResultScanner(
            new Scan(),
            primaryScannerMock,
            secondaryScannerWrapperMock,
            continuationFactoryMock,
            flowController,
            new MirroringTracer(),
            true);

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

    final ResultScanner mirroringScanner =
        new MirroringResultScanner(
            new Scan(),
            primaryScannerMock,
            secondaryScannerWrapperMock,
            continuationFactoryMock,
            flowController,
            new MirroringTracer(),
            true);

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

    final ResultScanner mirroringScanner =
        new MirroringResultScanner(
            new Scan(),
            primaryScannerMock,
            secondaryScannerWrapperMock,
            continuationFactoryMock,
            flowController,
            new MirroringTracer(),
            true);

    mirroringScanner.close();
    mirroringScanner.close();
    verify(primaryScannerMock, times(1)).close();
    verify(secondaryScannerWrapperMock, times(1)).asyncClose();
  }

  @Test
  public void testSecondaryNextsAreIssuedInTheSameOrderAsPrimary() throws IOException {
    AsyncResultScannerWrapper secondaryScannerWrapperMock = mock(AsyncResultScannerWrapper.class);
    AsyncTableWrapper secondaryAsyncTableWrapperMock = mock(AsyncTableWrapper.class);
    when(secondaryAsyncTableWrapperMock.getScanner(any(Scan.class)))
        .thenReturn(secondaryScannerWrapperMock);

    ResultScanner resultScanner = mock(ResultScanner.class);

    ReverseOrderExecutorService reverseOrderExecutorService = new ReverseOrderExecutorService();
    ListeningExecutorService listeningExecutorService =
        MoreExecutors.listeningDecorator(reverseOrderExecutorService);

    final AsyncResultScannerWrapper asyncResultScannerWrapper =
        new AsyncResultScannerWrapper(
            resultScanner, listeningExecutorService, new MirroringTracer());

    final List<ScannerRequestContext> calls = new ArrayList<>();

    Span span = Tracing.getTracer().spanBuilder("test").startSpan();

    ScannerRequestContext c1 = new ScannerRequestContext(null, null, 1, span);
    ScannerRequestContext c2 = new ScannerRequestContext(null, null, 2, span);
    ScannerRequestContext c3 = new ScannerRequestContext(null, null, 3, span);
    ScannerRequestContext c4 = new ScannerRequestContext(null, null, 4, span);
    ScannerRequestContext c5 = new ScannerRequestContext(null, null, 5, span);
    ScannerRequestContext c6 = new ScannerRequestContext(null, null, 6, span);

    catchResult(asyncResultScannerWrapper.next(c1).get(), calls);
    catchResult(asyncResultScannerWrapper.next(c2).get(), calls);
    catchResult(asyncResultScannerWrapper.next(c3).get(), calls);
    catchResult(asyncResultScannerWrapper.next(c4).get(), calls);
    catchResult(asyncResultScannerWrapper.next(c5).get(), calls);
    catchResult(asyncResultScannerWrapper.next(c6).get(), calls);

    reverseOrderExecutorService.callCallables();

    verify(resultScanner, times(6)).next();
    assertThat(calls).containsExactly(c1, c2, c3, c4, c5, c6);
  }

  private void catchResult(
      ListenableFuture<AsyncScannerVerificationPayload> next,
      final List<ScannerRequestContext> calls) {
    Futures.addCallback(
        next,
        new FutureCallback<AsyncScannerVerificationPayload>() {
          @Override
          public void onSuccess(
              @NullableDecl AsyncScannerVerificationPayload asyncScannerVerificationPayload) {
            calls.add(asyncScannerVerificationPayload.context);
          }

          @Override
          public void onFailure(Throwable throwable) {}
        },
        MoreExecutors.directExecutor());
  }

  static class ReverseOrderExecutorService implements ExecutorService {
    List<Runnable> callables = new ArrayList<>();

    public void callCallables() {
      for (int i = callables.size() - 1; i >= 0; i--) {
        callables.get(i).run();
      }
    }

    @Override
    public void shutdown() {}

    @Override
    public List<Runnable> shutdownNow() {
      throw new UnsupportedOperationException();
    }

    @Override
    public boolean isShutdown() {
      throw new UnsupportedOperationException();
    }

    @Override
    public boolean isTerminated() {
      throw new UnsupportedOperationException();
    }

    @Override
    public boolean awaitTermination(long l, TimeUnit timeUnit) throws InterruptedException {
      throw new UnsupportedOperationException();
    }

    @Override
    public <T> Future<T> submit(Callable<T> callable) {
      throw new UnsupportedOperationException();
    }

    @Override
    public <T> Future<T> submit(Runnable runnable, T t) {
      throw new UnsupportedOperationException();
    }

    @Override
    public Future<?> submit(Runnable runnable) {
      throw new UnsupportedOperationException();
    }

    @Override
    public <T> List<Future<T>> invokeAll(Collection<? extends Callable<T>> collection)
        throws InterruptedException {
      throw new UnsupportedOperationException();
    }

    @Override
    public <T> List<Future<T>> invokeAll(
        Collection<? extends Callable<T>> collection, long l, TimeUnit timeUnit)
        throws InterruptedException {
      return null;
    }

    @Override
    public <T> T invokeAny(Collection<? extends Callable<T>> collection)
        throws InterruptedException, ExecutionException {
      throw new UnsupportedOperationException();
    }

    @Override
    public <T> T invokeAny(Collection<? extends Callable<T>> collection, long l, TimeUnit timeUnit)
        throws InterruptedException, ExecutionException, TimeoutException {
      throw new UnsupportedOperationException();
    }

    @Override
    public void execute(Runnable runnable) {
      this.callables.add(runnable);
    }
  }
}
