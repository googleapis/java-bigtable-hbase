/*
 * Copyright 2016 Google Inc. All Rights Reserved.
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
package com.google.cloud.bigtable.dataflow;

import static org.mockito.Mockito.when;

import java.io.IOException;

import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import com.google.bigtable.repackaged.com.google.cloud.grpc.BigtableSession;
import com.google.bigtable.repackaged.com.google.cloud.grpc.scanner.ResultScanner;
import com.google.bigtable.repackaged.com.google.com.google.bigtable.v2.Row;
import com.google.bigtable.repackaged.com.google.protobuf.ByteString;

/**
 * Tests for {@link CloudBigtableIO.Reader}.
 * @author sduskis
 *
 */
public class CloudBigtableIOReaderTest {

  @Mock
  BigtableSession mockSession;

  @Mock
  ResultScanner<Row> mockScanner;

  @Mock
  CloudBigtableIO.AbstractSource<Result> mockSource;

  @Before
  public void setup(){
    MockitoAnnotations.initMocks(this);
    CloudBigtableScanConfiguration config = new CloudBigtableScanConfiguration.Builder()
        .withProjectId("test").withInstanceId("test").withTableId("test").build();
    when(mockSource.getConfiguration()).thenReturn(config);
  }

  @Test
  public void testBasic() throws IOException {
    CloudBigtableIO.Reader<Result> underTest =
        new CloudBigtableIO.Reader<Result>(mockSource, CloudBigtableIO.RESULT_ADVANCER) {
          @Override
          void initializeScanner() throws IOException {
            setSession(mockSession);
            setScanner(mockScanner);
          }
        };

    ByteString key = ByteString.copyFrom(Bytes.toBytes("a"));
    when(mockScanner.next()).thenReturn(Row.newBuilder().setKey(key).build());
    Assert.assertTrue(underTest.start());
    Assert.assertEquals(1, underTest.getRowsReadCount());

    when(mockScanner.next()).thenReturn(null);
    Assert.assertFalse(underTest.advance());
    Assert.assertEquals(1, underTest.getRowsReadCount());

    underTest.close();
  }
}