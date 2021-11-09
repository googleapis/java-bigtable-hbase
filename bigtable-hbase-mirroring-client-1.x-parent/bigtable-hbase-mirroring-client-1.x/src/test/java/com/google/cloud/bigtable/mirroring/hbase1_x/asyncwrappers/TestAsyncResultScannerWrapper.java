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
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import com.google.cloud.bigtable.mirroring.hbase1_x.utils.mirroringmetrics.MirroringTracer;
import com.google.common.util.concurrent.MoreExecutors;
import com.google.common.util.concurrent.SettableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
public class TestAsyncResultScannerWrapper {
  @Test
  public void testListenersAreCalledOnClose()
      throws InterruptedException, ExecutionException, TimeoutException {
    ResultScanner resultScanner = mock(ResultScanner.class);
    AsyncResultScannerWrapper asyncResultScannerWrapper =
        new AsyncResultScannerWrapper(
            resultScanner,
            MoreExecutors.listeningDecorator(MoreExecutors.newDirectExecutorService()),
            new MirroringTracer());
    final SettableFuture<Void> listenerFuture = SettableFuture.create();
    asyncResultScannerWrapper.addOnCloseListener(
        new Runnable() {
          @Override
          public void run() {
            listenerFuture.set(null);
          }
        });
    asyncResultScannerWrapper.asyncClose().get(3, TimeUnit.SECONDS);
    assertThat(listenerFuture.get(3, TimeUnit.SECONDS)).isNull();
  }

  @Test
  public void testAsyncResultScannerWrapperClosedTwiceClosesScannerOnce()
      throws InterruptedException, ExecutionException, TimeoutException {
    ResultScanner resultScanner = mock(ResultScanner.class);
    AsyncResultScannerWrapper asyncResultScannerWrapper =
        new AsyncResultScannerWrapper(
            resultScanner,
            MoreExecutors.listeningDecorator(MoreExecutors.newDirectExecutorService()),
            new MirroringTracer());
    asyncResultScannerWrapper.asyncClose().get(3, TimeUnit.SECONDS);
    asyncResultScannerWrapper.asyncClose().get(3, TimeUnit.SECONDS);
    verify(resultScanner, times(1)).close();
  }
}
