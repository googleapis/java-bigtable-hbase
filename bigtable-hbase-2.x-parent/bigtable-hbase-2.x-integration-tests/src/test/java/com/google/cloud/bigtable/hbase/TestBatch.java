/*
 * Copyright 2015 Google LLC
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

import static org.junit.Assert.assertNull;
import static org.mockito.Mockito.verify;

import java.util.Collections;
import org.apache.hadoop.hbase.client.Append;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.client.coprocessor.Batch;
import org.junit.Test;
import org.mockito.Mockito;

public class TestBatch extends AbstractTestBatch {
  protected void appendAdd(Append append, byte[] columnFamily, byte[] qualifier, byte[] value) {
    append.addColumn(columnFamily, qualifier, value);
  }

  @Test
  @SuppressWarnings("unchecked")
  public void testBatchWithMismatchedResultArray() throws Exception {
    Table table = getDefaultTable();
    Exception actualError = null;
    Batch.Callback mockCallBack = Mockito.mock(Batch.Callback.class);
    try {
      // This is accepted behaviour in HBase 2 API, It ignores the `new Object[1]` param.
      table.batchCallback(Collections.emptyList(), new Object[1], mockCallBack);
    } catch (Exception ex) {
      actualError = ex;
    }
    assertNull(actualError);
    verify(mockCallBack, Mockito.never()).update(Mockito.any(), Mockito.any(), Mockito.any());
  }
}
