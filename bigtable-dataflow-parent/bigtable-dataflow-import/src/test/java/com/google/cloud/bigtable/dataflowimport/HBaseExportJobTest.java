/*
 * Copyright (C) 2017 Google Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */
package com.google.cloud.bigtable.dataflowimport;

import static org.mockito.Matchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.google.api.client.util.Clock;
import com.google.cloud.bigtable.dataflowimport.HBaseExportJob.SequenceFileFactory;
import com.google.cloud.bigtable.dataflowimport.HBaseExportJob.WriteResultsToSeq;
import com.google.cloud.bigtable.dataflowimport.testing.HBaseCellUtils;
import com.google.cloud.dataflow.sdk.io.BoundedSource;
import com.google.cloud.dataflow.sdk.io.BoundedSource.BoundedReader;
import com.google.cloud.dataflow.sdk.options.PipelineOptions;
import com.google.cloud.dataflow.sdk.transforms.DoFnTester;
import com.google.cloud.dataflow.sdk.transforms.DoFnTester.CloningBehavior;
import com.google.cloud.dataflow.sdk.values.KV;
import java.io.IOException;
import java.util.Arrays;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.io.MapFile.Writer.Option;
import org.apache.hadoop.io.SequenceFile;
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

@RunWith(JUnit4.class)
public class HBaseExportJobTest {

  @Mock
  BoundedSource<Result> mockSource;
  @Mock
  BoundedReader<Result> mockReader;
  @Mock
  SequenceFileFactory mockSequenceFileFactory;
  @Mock
  SequenceFile.Writer mockWriter;
  @Mock
  Clock clock;

  @Before
  public void setup() throws IOException {
    MockitoAnnotations.initMocks(this);

    when(mockSource.createReader(any(PipelineOptions.class)))
        .thenReturn(mockReader);
    when(mockSequenceFileFactory.createWriter(any(Configuration.class), Mockito.<Option>anyVararg()))
        .thenReturn(mockWriter);
  }

  @Test
  public void testElementPassthru() throws IOException {
    // 1 source with 1 result in it
    Result result = Result.create(Arrays.asList(
        HBaseCellUtils.createDataCell("key1".getBytes(), "cf".getBytes(), "".getBytes(), 10)
    ));
    when(mockReader.start()).thenReturn(true);
    when(mockReader.advance()).thenReturn(false);
    when(mockReader.getCurrent()).thenReturn(result);

    WriteResultsToSeq doFn = new WriteResultsToSeq("/fakePath", mockSequenceFileFactory, clock);
    DoFnTester<KV<String, BoundedSource<Result>>, Void> tester = DoFnTester.of(doFn);
    tester.setCloningBehavior(CloningBehavior.DO_NOT_CLONE);

    tester.processElement(KV.of("key", mockSource));
    verify(mockWriter).append(new ImmutableBytesWritable(result.getRow()), result);
    verify(mockWriter).close();
  }


  @Test
  public void testCounters() throws IOException {
    WriteResultsToSeq doFn = new WriteResultsToSeq("/fakePath", mockSequenceFileFactory, clock);
    DoFnTester<KV<String, BoundedSource<Result>>, Void> tester = DoFnTester.of(doFn);
    tester.setCloningBehavior(CloningBehavior.DO_NOT_CLONE);

    // 1 source with 1 result in it
    Result result = Result.create(Arrays.asList(
        HBaseCellUtils.createDataCell("key1".getBytes(), "cf".getBytes(), "".getBytes(), 10)
    ));

    // 0-3s read
    when(mockReader.start()).then(new Answer<Boolean>() {
      @Override
      public Boolean answer(InvocationOnMock invocation) throws Throwable {
        when(clock.currentTimeMillis()).thenReturn(3000L);
        return true;
      }
    });
    when(mockReader.getCurrent()).thenReturn(result);

    // 3-7 write
    doAnswer(new Answer() {
      @Override
      public Object answer(InvocationOnMock invocation) throws Throwable {
        when(clock.currentTimeMillis()).thenReturn(7000L);
        return null;
      }
    }).when(mockWriter).append(any(ImmutableBytesWritable.class), any(Result.class));

    // 7 - 12 advance (read)
    when(mockReader.advance())
        .thenAnswer(new Answer<Boolean>() {
          @Override
          public Boolean answer(InvocationOnMock invocation) throws Throwable {
            when(clock.currentTimeMillis()).thenReturn(12000L);
            return false;
          }
        });

    when(mockWriter.getLength()).thenReturn(37L * 1024 * 1024);

    tester.processElement(KV.of("key", mockSource));

    Assert.assertEquals(new Long(1), tester.getAggregatorValue(doFn.itemCounter));
    Assert.assertEquals(new Long(3 + 5), tester.getAggregatorValue(doFn.readDuration.aggregator));
    Assert.assertEquals(new Long(4), tester.getAggregatorValue(doFn.writeDuration.aggregator));
    Assert.assertEquals(new Long(12), tester.getAggregatorValue(doFn.totalDuration.aggregator));
    Assert.assertEquals(new Long(37), tester.getAggregatorValue(doFn.bytesWritten.aggregator));
  }
}
