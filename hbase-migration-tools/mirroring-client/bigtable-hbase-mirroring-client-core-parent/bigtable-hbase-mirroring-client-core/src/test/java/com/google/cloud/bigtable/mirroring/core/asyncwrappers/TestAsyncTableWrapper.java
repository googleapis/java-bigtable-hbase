/*
 * Copyright 2022 Google LLC
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
package com.google.cloud.bigtable.mirroring.core.asyncwrappers;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import com.google.cloud.bigtable.mirroring.core.utils.mirroringmetrics.MirroringTracer;
import com.google.common.util.concurrent.ListeningExecutorService;
import java.io.IOException;
import org.apache.hadoop.hbase.client.Table;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
public class TestAsyncTableWrapper {

  @Test
  public void testMultipleCloseCallsCloseOnTableOnlyOnce() throws IOException {
    Table table = mock(Table.class);
    AsyncTableWrapper asyncTableWrapper =
        new AsyncTableWrapper(table, mock(ListeningExecutorService.class), new MirroringTracer());
    asyncTableWrapper.close();
    asyncTableWrapper.close();
    verify(table, times(1)).close();
  }
}
