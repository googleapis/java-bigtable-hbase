/*
 * Copyright (C) 2015 Google Inc.
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
package com.google.cloud.bigtable.dataflow;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.when;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.apache.hadoop.hbase.client.Append;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Increment;
import org.apache.hadoop.hbase.client.Mutation;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import com.google.bigtable.repackaged.com.google.protobuf.ByteString;
import com.google.bigtable.v1.SampleRowKeysResponse;
import com.google.cloud.bigtable.dataflow.CloudBigtableIO.AbstractSource;
import com.google.cloud.bigtable.dataflow.CloudBigtableIO.SourceWithKeys;
import com.google.cloud.dataflow.sdk.Pipeline;
import com.google.cloud.dataflow.sdk.coders.CannotProvideCoderException;
import com.google.cloud.dataflow.sdk.coders.Coder;
import com.google.cloud.dataflow.sdk.coders.CoderRegistry;
import com.google.cloud.dataflow.sdk.io.BoundedSource;
import com.google.cloud.dataflow.sdk.values.TypeDescriptor;

/**
 * Tests for {@link CloudBigtableIO}.
 */
public class CloudBigtableIOTest {

  @Rule
  public ExpectedException expectedException = ExpectedException.none();

  @Mock
  Pipeline underTest;

  @Mock
  CloudBigtableOptions cbtOptions;

  private CoderRegistry registry = new CoderRegistry();

  @SuppressWarnings("unchecked")
  @Before
  public void setup(){
    MockitoAnnotations.initMocks(this);
    when(underTest.getOptions()).thenReturn(cbtOptions);
    when(underTest.getCoderRegistry()).thenReturn(registry);
    when(cbtOptions.as(any(Class.class))).thenReturn(cbtOptions);
    CloudBigtableIO.initializeForWrite(underTest);
  }

  private void checkRegistry(Class<? extends Mutation> mutationClass)
      throws CannotProvideCoderException {
    Coder<? extends Mutation> coder = registry.getCoder(TypeDescriptor.of(mutationClass));
    assertNotNull(coder);
    assertEquals(HBaseMutationCoder.class, coder.getClass());
  }

  @Test
  public void testInitialize() throws Exception{
    checkRegistry(Put.class);
    checkRegistry(Delete.class);
    checkRegistry(Mutation.class);
  }

  @Test
  public void testAppendThrowsCannotProvideCoderException() throws Exception {
    expectedException.expect(CannotProvideCoderException.class);
    registry.getCoder(TypeDescriptor.of(Append.class));
  }

  @Test
  public void testIncrementThrowsCannotProvideCoderException() throws Exception {
    expectedException.expect(CannotProvideCoderException.class);
    registry.getCoder(TypeDescriptor.of(Increment.class));
  }

  @Test
  public void testSourceToString() throws Exception {
    CloudBigtableIO.Source<Result> source = createSource();
    byte[] startKey = "abc d".getBytes();
    byte[] stopKey = "def g".getBytes();
    BoundedSource<Result> sourceWithKeys = source.createSourceWithKeys(startKey, stopKey, 10);
    assertEquals("Split start: 'abc d', end: 'def g', size: 10", sourceWithKeys.toString());

    startKey = new byte[]{0, 1, 2, 3, 4, 5};
    stopKey = new byte[]{104, 101, 108, 108, 111};  // hello
    sourceWithKeys = source.createSourceWithKeys(startKey, stopKey, 10);
    assertEquals("Split start: '\\x00\\x01\\x02\\x03\\x04\\x05', end: 'hello', size: 10",
        sourceWithKeys.toString());
  }

  protected CloudBigtableScanConfiguration createConfig() {
    return new CloudBigtableScanConfiguration.Builder().withProjectId("project")
        .withZoneId("zoneId").withClusterId("clusterId").withTableId("tableId").build();
  }

  @SuppressWarnings({ "rawtypes", "unchecked" })
  @Test
  public void testSampleRowKeys() throws Exception {
    List<SampleRowKeysResponse> sampleRowKeys = new ArrayList<>();
    int count = (int) (AbstractSource.COUNT_MAX_SPLIT_COUNT * 3 - 5);
    byte[][] keys = Bytes.split("A".getBytes(), "Z".getBytes(), count-2);
    long tabletSize = 2L * 1024L * 1024L * 1024L;
    long boundary = 0;
    for (byte[] currentKey : keys) {
      boundary += tabletSize;
      try {
        sampleRowKeys.add(SampleRowKeysResponse.newBuilder()
          .setRowKey(ByteString.copyFrom(currentKey))
          .setOffsetBytes(boundary)
          .build());
      } catch (NoClassDefFoundError e) {
        // This could cause some problems for javadoc or cobertura because of the shading magic we
        // do.
        e.printStackTrace();
        return;
      }
    }
    CloudBigtableIO.Source source = createSource();
    source.setSampleRowKeys(sampleRowKeys);
    List<CloudBigtableIO.SourceWithKeys> splits = source.getSplits(20000);
    Assert.assertTrue(splits.size() <= CloudBigtableIO.AbstractSource.COUNT_MAX_SPLIT_COUNT);
    Iterator<SourceWithKeys> iter = splits.iterator();
    SourceWithKeys last = iter.next();
    while(iter.hasNext()) {
      SourceWithKeys current = iter.next();
      Assert.assertTrue(Bytes.equals(current.getStartRow(), last.getStopRow()));
      // The last source will have a stop key of empty.
      if (iter.hasNext()) {
        Assert.assertTrue(Bytes.compareTo(current.getStartRow(), current.getStopRow()) < 0);
      }
      Assert.assertTrue(current.getEstimatedSize() >= tabletSize);
      last = current;
    }
    // check first and last
  }

  protected CloudBigtableIO.Source<Result> createSource() {
    return (CloudBigtableIO.Source<Result>) CloudBigtableIO.read(createConfig());
  }
}
