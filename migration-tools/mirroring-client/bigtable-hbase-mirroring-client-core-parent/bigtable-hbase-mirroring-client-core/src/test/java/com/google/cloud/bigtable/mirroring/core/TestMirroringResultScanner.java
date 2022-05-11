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
package com.google.cloud.bigtable.mirroring.core;

import static com.google.common.truth.Truth.assertThat;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.fail;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.google.cloud.bigtable.mirroring.core.MirroringTable.RequestScheduler;
import com.google.cloud.bigtable.mirroring.core.asyncwrappers.AsyncResultScannerWrapper;
import com.google.cloud.bigtable.mirroring.core.asyncwrappers.AsyncResultScannerWrapper.AsyncScannerVerificationPayload;
import com.google.cloud.bigtable.mirroring.core.asyncwrappers.AsyncResultScannerWrapper.ScannerRequestContext;
import com.google.cloud.bigtable.mirroring.core.asyncwrappers.AsyncTableWrapper;
import com.google.cloud.bigtable.mirroring.core.utils.flowcontrol.FlowController;
import com.google.cloud.bigtable.mirroring.core.utils.mirroringmetrics.MirroringTracer;
import com.google.cloud.bigtable.mirroring.core.utils.referencecounting.ListenableReferenceCounter;
import com.google.cloud.bigtable.mirroring.core.utils.referencecounting.ReferenceCounter;
import com.google.cloud.bigtable.mirroring.core.verification.MismatchDetector;
import com.google.cloud.bigtable.mirroring.core.verification.VerificationContinuationFactory;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.ListeningExecutorService;
import com.google.common.util.concurrent.MoreExecutors;
import io.opencensus.trace.Span;
import io.opencensus.trace.Tracing;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
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
import org.junit.Before;
import org.junit.Test;
import org.junit.function.ThrowingRunnable;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.mockito.Mock;

@RunWith(JUnit4.class)
public class TestMirroringResultScanner {
  @Mock FlowController flowController;
  VerificationContinuationFactory continuationFactoryMock =
      mock(VerificationContinuationFactory.class);

  @Before
  public void setUp() {
    MismatchDetector mismatchDetectorMock = mock(MismatchDetector.class);
    when(mismatchDetectorMock.createScannerResultVerifier(any(Scan.class), anyInt()))
        .thenReturn(mock(MismatchDetector.ScannerResultVerifier.class));

    when(continuationFactoryMock.getMismatchDetector()).thenReturn(mismatchDetectorMock);
  }

  @Test
  public void testScannerCloseWhenFirstCloseThrows() throws IOException {
    ResultScanner primaryScannerMock = mock(ResultScanner.class);

    AsyncResultScannerWrapper secondaryScannerWrapperMock = mock(AsyncResultScannerWrapper.class);

    final ResultScanner mirroringScanner =
        new MirroringResultScanner(
            new Scan(),
            primaryScannerMock,
            secondaryScannerWrapperMock,
            continuationFactoryMock,
            new MirroringTracer(),
            true,
            new RequestScheduler(
                flowController, new MirroringTracer(), mock(ListenableReferenceCounter.class)),
            mock(ReferenceCounter.class),
            10);
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
    verify(secondaryScannerWrapperMock, times(1)).close();
    assertThat(thrown).hasMessageThat().contains("first");
  }

  @Test
  public void testScannerCloseWhenSecondCloseThrows()
      throws TimeoutException, InterruptedException {
    ResultScanner primaryScannerMock = mock(ResultScanner.class);

    AsyncResultScannerWrapper secondaryScannerWrapperMock = mock(AsyncResultScannerWrapper.class);

    final MirroringResultScanner mirroringScanner =
        new MirroringResultScanner(
            new Scan(),
            primaryScannerMock,
            secondaryScannerWrapperMock,
            continuationFactoryMock,
            new MirroringTracer(),
            true,
            new RequestScheduler(
                flowController, new MirroringTracer(), mock(ListenableReferenceCounter.class)),
            mock(ReferenceCounter.class),
            10);

    doThrow(new RuntimeException("second")).when(secondaryScannerWrapperMock).close();

    mirroringScanner.close();

    verify(primaryScannerMock, times(1)).close();
    verify(secondaryScannerWrapperMock, times(1)).close();
    try {
      mirroringScanner.closePrimaryAndScheduleSecondaryClose().get(3, TimeUnit.SECONDS);
    } catch (ExecutionException e) {
      assertThat(e).hasCauseThat().hasMessageThat().contains("second");
    }
  }

  @Test
  public void testScannerCloseWhenBothCloseThrow() throws InterruptedException, TimeoutException {
    ResultScanner primaryScannerMock = mock(ResultScanner.class);

    AsyncResultScannerWrapper secondaryScannerWrapperMock = mock(AsyncResultScannerWrapper.class);

    final MirroringResultScanner mirroringScanner =
        new MirroringResultScanner(
            new Scan(),
            primaryScannerMock,
            secondaryScannerWrapperMock,
            continuationFactoryMock,
            new MirroringTracer(),
            true,
            new RequestScheduler(
                flowController, new MirroringTracer(), mock(ListenableReferenceCounter.class)),
            mock(ReferenceCounter.class),
            10);

    doThrow(new RuntimeException("first")).when(primaryScannerMock).close();
    doThrow(new RuntimeException("second")).when(secondaryScannerWrapperMock).close();

    RuntimeException thrown =
        assertThrows(
            RuntimeException.class,
            new ThrowingRunnable() {
              @Override
              public void run() {
                mirroringScanner.closePrimaryAndScheduleSecondaryClose();
              }
            });

    // asyncClose returns future that will resolve to secondary error.
    // Second call to closePrimaryAndScheduleSecondaryClose() should perform any other operation.
    ListenableFuture<Void> asyncCloseResult =
        mirroringScanner.closePrimaryAndScheduleSecondaryClose();

    verify(primaryScannerMock, times(1)).close();
    assertThat(thrown).hasMessageThat().contains("first");
    try {
      asyncCloseResult.get(3, TimeUnit.SECONDS);
      fail();
    } catch (ExecutionException e) {
      assertThat(e).hasCauseThat().hasMessageThat().contains("second");
    }

    verify(secondaryScannerWrapperMock, times(1)).close();
  }

  @Test
  public void testMultipleCloseCallsCloseScannersOnlyOnce() throws IOException {
    ResultScanner primaryScannerMock = mock(ResultScanner.class);

    AsyncResultScannerWrapper secondaryScannerWrapperMock = mock(AsyncResultScannerWrapper.class);

    final ResultScanner mirroringScanner =
        new MirroringResultScanner(
            new Scan(),
            primaryScannerMock,
            secondaryScannerWrapperMock,
            continuationFactoryMock,
            new MirroringTracer(),
            true,
            new RequestScheduler(
                flowController, new MirroringTracer(), mock(ListenableReferenceCounter.class)),
            mock(ReferenceCounter.class),
            10);

    mirroringScanner.close();
    mirroringScanner.close();
    verify(primaryScannerMock, times(1)).close();
    verify(secondaryScannerWrapperMock, times(1)).close();
  }

  @Test
  public void testSecondaryNextsAreIssuedInTheSameOrderAsPrimary() throws IOException {
    // AsyncRequestWrapper has a concurrent queue of primary scanner results (with some context).
    // When a next() is requested, it puts its context into the queue.
    // Then it acquires a mutex and while holding it pops a context from the queue
    // and then runs next() on underlying ResultScanner from secondary database.
    // Later it joins the primary and secondary results in an object further passed to
    // MismatchDetector.
    // This test proves that even if the asynchronous requests get reordered, the
    // queue is emptied in order (so that results of primary and secondary scanner
    // are paired as intended).

    AsyncResultScannerWrapper secondaryScannerWrapperMock = mock(AsyncResultScannerWrapper.class);
    AsyncTableWrapper secondaryAsyncTableWrapperMock = mock(AsyncTableWrapper.class);
    when(secondaryAsyncTableWrapperMock.getScanner(any(Scan.class)))
        .thenReturn(secondaryScannerWrapperMock);

    ResultScanner resultScanner = mock(ResultScanner.class);

    // We force reordering of secondary requests.
    ReverseOrderExecutorService reverseOrderExecutorService = new ReverseOrderExecutorService();
    ListeningExecutorService listeningExecutorService =
        MoreExecutors.listeningDecorator(reverseOrderExecutorService);

    final AsyncResultScannerWrapper asyncResultScannerWrapper =
        new AsyncResultScannerWrapper(
            resultScanner, listeningExecutorService, new MirroringTracer());

    final List<ScannerRequestContext> calls = new ArrayList<>();

    Span span = Tracing.getTracer().spanBuilder("test").startSpan();

    List<ScannerRequestContext> contexts =
        Arrays.asList(
            new ScannerRequestContext(null, null, 1, span),
            new ScannerRequestContext(null, null, 2, span),
            new ScannerRequestContext(null, null, 3, span),
            new ScannerRequestContext(null, null, 4, span),
            new ScannerRequestContext(null, null, 5, span),
            new ScannerRequestContext(null, null, 6, span));

    for (ScannerRequestContext ctx : contexts) {
      asyncResultScannerWrapper.next(ctx).get();
    }

    reverseOrderExecutorService.callScheduledCallables();
    verify(resultScanner, times(6)).next(anyInt());

    for (int i = 0; i < contexts.size(); i++) {
      assertThat(asyncResultScannerWrapper.nextResultQueue.remove().context)
          .isEqualTo(contexts.get(i));
    }
  }

  private FutureCallback<AsyncScannerVerificationPayload> addContextToListCallback(
      final List<ScannerRequestContext> list) {
    return new FutureCallback<AsyncScannerVerificationPayload>() {
      @Override
      public void onSuccess(
          @NullableDecl AsyncScannerVerificationPayload asyncScannerVerificationPayload) {
        list.add(asyncScannerVerificationPayload.context);
      }

      @Override
      public void onFailure(Throwable throwable) {}
    };
  }

  static class ReverseOrderExecutorService implements ExecutorService {

    List<Runnable> callables = new ArrayList<>();

    public void callScheduledCallables() {
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
