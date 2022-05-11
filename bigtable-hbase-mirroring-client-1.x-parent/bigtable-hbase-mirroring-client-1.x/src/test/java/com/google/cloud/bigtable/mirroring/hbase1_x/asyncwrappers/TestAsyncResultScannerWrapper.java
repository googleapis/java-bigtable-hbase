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

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import com.google.cloud.bigtable.mirroring.hbase1_x.utils.mirroringmetrics.MirroringTracer;
import com.google.common.util.concurrent.MoreExecutors;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
public class TestAsyncResultScannerWrapper {
  @Test
  public void testAsyncResultScannerWrapperClosedTwiceClosesScannerOnce() {
    ResultScanner resultScanner = mock(ResultScanner.class);
    AsyncResultScannerWrapper asyncResultScannerWrapper =
        new AsyncResultScannerWrapper(
            resultScanner,
            MoreExecutors.listeningDecorator(MoreExecutors.newDirectExecutorService()),
            new MirroringTracer());
    asyncResultScannerWrapper.close();
    asyncResultScannerWrapper.close();
    verify(resultScanner, times(1)).close();
  }
}
