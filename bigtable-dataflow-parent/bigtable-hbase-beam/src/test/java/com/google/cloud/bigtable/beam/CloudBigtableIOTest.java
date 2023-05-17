/*
 * Copyright 2017 Google Inc.
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
package com.google.cloud.bigtable.beam;

import static org.apache.beam.sdk.options.ValueProvider.StaticValueProvider;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.mockito.Mockito.when;

import com.google.bigtable.repackaged.com.google.cloud.bigtable.data.v2.internal.ByteStringComparator;
import com.google.bigtable.repackaged.com.google.cloud.bigtable.data.v2.models.KeyOffset;
import com.google.bigtable.repackaged.com.google.protobuf.ByteString;
import com.google.cloud.bigtable.beam.CloudBigtableIO.AbstractSource;
import com.google.cloud.bigtable.beam.CloudBigtableIO.Source;
import com.google.cloud.bigtable.beam.CloudBigtableIO.SourceWithKeys;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.coders.CannotProvideCoderException;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.CoderRegistry;
import org.apache.beam.sdk.io.BoundedSource;
import org.apache.beam.sdk.util.SerializableUtils;
import org.apache.beam.sdk.values.TypeDescriptor;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Mutation;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.KeyOnlyFilter;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnit;
import org.mockito.junit.MockitoRule;

/** Tests for {@link CloudBigtableIO}. */
@RunWith(JUnit4.class)
public class CloudBigtableIOTest {
  @Rule public final MockitoRule mockitoRule = MockitoJUnit.rule();

  @Rule public ExpectedException expectedException = ExpectedException.none();

  @Mock Pipeline underTest;

  private static final CoderRegistry registry = CoderRegistry.createDefault();

  private CloudBigtableScanConfiguration scanConfig =
      new CloudBigtableScanConfiguration.Builder()
          .withProjectId("project")
          .withInstanceId("instanceId")
          .withTableId("table")
          .build();

  @Before
  public void setup() {
    when(underTest.getCoderRegistry()).thenReturn(registry);
  }

  private void checkRegistry(Class<? extends Mutation> mutationClass)
      throws CannotProvideCoderException {
    Coder<? extends Mutation> coder = registry.getCoder(TypeDescriptor.of(mutationClass));
    assertNotNull(coder);
  }

  @Test
  public void testInitialize() throws Exception {
    checkRegistry(Put.class);
    checkRegistry(Delete.class);
    checkRegistry(Mutation.class);
  }

  @Test
  public void testSourceToString() throws Exception {
    Source source = (Source) CloudBigtableIO.read(scanConfig);
    byte[] startKey = "abc d".getBytes();
    byte[] stopKey = "def g".getBytes();
    BoundedSource<Result> sourceWithKeys = source.createSourceWithKeys(startKey, stopKey, 10);
    assertEquals("Split start: 'abc d', end: 'def g', size: 10.", sourceWithKeys.toString());

    startKey = new byte[] {0, 1, 2, 3, 4, 5};
    stopKey = new byte[] {104, 101, 108, 108, 111}; // hello
    sourceWithKeys = source.createSourceWithKeys(startKey, stopKey, 10);
    assertEquals(
        "Split start: '\\x00\\x01\\x02\\x03\\x04\\x05', end: 'hello', size: 10.",
        sourceWithKeys.toString());
  }

  @Test
  public void testSampleRowKeys() throws Exception {
    List<KeyOffset> sampleRowKeys = new ArrayList<>();
    int count = (int) (AbstractSource.COUNT_MAX_SPLIT_COUNT * 3 - 5);
    byte[][] keys = Bytes.split("A".getBytes(), "Z".getBytes(), count - 2);
    long tabletSize = 2L * 1024L * 1024L * 1024L;
    long boundary = 0;
    for (byte[] currentKey : keys) {
      boundary += tabletSize;
      try {
        sampleRowKeys.add(KeyOffset.create(ByteString.copyFrom(currentKey), boundary));
      } catch (NoClassDefFoundError e) {
        // This could cause some problems for javadoc or cobertura because of the shading magic we
        // do.
        e.printStackTrace();
        return;
      }
    }
    Source source = (Source) CloudBigtableIO.read(scanConfig);
    source.setSampleRowKeys(sampleRowKeys);
    List<SourceWithKeys> splits = source.getSplits(20000);
    Collections.sort(
        splits,
        new Comparator<SourceWithKeys>() {
          @Override
          public int compare(SourceWithKeys o1, SourceWithKeys o2) {
            return ByteStringComparator.INSTANCE.compare(
                ByteString.copyFrom(o1.getConfiguration().getStartRow()),
                ByteString.copyFrom(o2.getConfiguration().getStartRow()));
          }
        });
    Assert.assertTrue(splits.size() <= AbstractSource.COUNT_MAX_SPLIT_COUNT);
    Iterator<SourceWithKeys> iter = splits.iterator();
    SourceWithKeys last = iter.next();
    while (iter.hasNext()) {
      SourceWithKeys current = iter.next();
      Assert.assertTrue(
          Bytes.equals(
              current.getConfiguration().getStartRow(), last.getConfiguration().getStopRow()));
      // The last source will have a stop key of empty.
      if (iter.hasNext()) {
        Assert.assertTrue(
            Bytes.compareTo(
                    current.getConfiguration().getStartRow(),
                    current.getConfiguration().getStopRow())
                < 0);
      }
      Assert.assertTrue(current.getEstimatedSize() >= tabletSize);
      last = current;
    }
    // check first and last
  }

  @Test
  public void testWriteToTableValidateConfig() throws Exception {
    // No error.
    CloudBigtableIO.writeToTable(scanConfig).validate(null);

    // Empty project ID.
    try {
      CloudBigtableIO.writeToTable(scanConfig.toBuilder().withProjectId("").build()).validate(null);
      Assert.fail("Expect IllegalArgumentException");
    } catch (IllegalArgumentException e) {
      Assert.assertTrue(e.getMessage().contains("A projectId must be set"));
    }

    // Empty instance ID.
    try {
      CloudBigtableIO.writeToTable(scanConfig.toBuilder().withInstanceId("").build())
          .validate(null);
      Assert.fail("Expect IllegalArgumentException");
    } catch (IllegalArgumentException e) {
      Assert.assertTrue(e.getMessage().contains("A instanceId must be set"));
    }

    // Empty table ID.
    try {
      CloudBigtableIO.writeToTable(scanConfig.toBuilder().withTableId("").build()).validate(null);
      Assert.fail("Expect IllegalArgumentException");
    } catch (IllegalArgumentException e) {
      Assert.assertTrue(e.getMessage().contains("A tableid must be set"));
    }
  }

  @Test
  public void testWriteToMultipleTablesValidateConfig() throws Exception {
    CloudBigtableConfiguration config =
        new CloudBigtableConfiguration.Builder()
            .withProjectId("project")
            .withInstanceId("instanceId")
            .build();

    // No error.
    CloudBigtableIO.writeToMultipleTables(config).validate(null);

    // Empty project ID.
    try {
      CloudBigtableIO.writeToMultipleTables(config.toBuilder().withProjectId("").build())
          .validate(null);
      Assert.fail("Expect IllegalArgumentException");
    } catch (IllegalArgumentException e) {
      Assert.assertTrue(e.getMessage().contains("A projectId must be set"));
    }

    // Empty instance ID.
    try {
      CloudBigtableIO.writeToMultipleTables(config.toBuilder().withInstanceId("").build())
          .validate(null);
      Assert.fail("Expect IllegalArgumentException");
    } catch (IllegalArgumentException e) {
      Assert.assertTrue(e.getMessage().contains("A instanceId must be set"));
    }
  }

  @Test
  public void testSerializeSource() {
    String projectId = "my-project";
    String instanceId = "my-instance";
    String tableId = "my-table";
    Scan scan = new Scan().setFilter(new KeyOnlyFilter()).setMaxVersions(2);
    CloudBigtableScanConfiguration configuration =
        CloudBigtableScanConfiguration.createConfig(
            StaticValueProvider.of(projectId),
            StaticValueProvider.of(instanceId),
            StaticValueProvider.of(tableId),
            StaticValueProvider.of(scan),
            new HashMap<>());

    Source source = new Source(configuration);

    Source deserialized = SerializableUtils.ensureSerializable(source);

    assertEquals(projectId, deserialized.getConfiguration().getProjectId());
    assertEquals(instanceId, deserialized.getConfiguration().getInstanceId());
    assertEquals(tableId, deserialized.getConfiguration().getTableId());
    assertEquals(
        scan.getFilter(), deserialized.getConfiguration().getScanValueProvider().get().getFilter());
    assertEquals(2, scan.getMaxVersions());
  }

  @Test
  public void testSerializeSourceWithKeys() {
    String projectId = "my-project";
    String instanceId = "my-instance";
    String tableId = "my-table";
    String startKey = "aaa1";
    String endKey = "bbb3";

    Scan scan =
        new Scan()
            .withStartRow(ByteString.copyFromUtf8(startKey).toByteArray())
            .withStopRow(ByteString.copyFromUtf8(endKey).toByteArray())
            .setFilter(new KeyOnlyFilter());

    CloudBigtableScanConfiguration sourceWithKeysConfiguration =
        CloudBigtableScanConfiguration.createConfig(
            StaticValueProvider.of(projectId),
            StaticValueProvider.of(instanceId),
            StaticValueProvider.of(tableId),
            StaticValueProvider.of(scan),
            new HashMap<>());

    long estimatedSize = 123456;
    SourceWithKeys sourceWithKeys = new SourceWithKeys(sourceWithKeysConfiguration, estimatedSize);

    SourceWithKeys deserialized = SerializableUtils.ensureSerializable(sourceWithKeys);

    assertEquals(projectId, deserialized.getConfiguration().getProjectId());
    assertEquals(instanceId, deserialized.getConfiguration().getInstanceId());
    assertEquals(tableId, deserialized.getConfiguration().getTableId());
    assertEquals(
        startKey,
        ByteString.copyFrom(
                deserialized.getConfiguration().getScanValueProvider().get().getStartRow())
            .toStringUtf8());
    assertEquals(
        endKey,
        ByteString.copyFrom(
                deserialized.getConfiguration().getScanValueProvider().get().getStopRow())
            .toStringUtf8());
    assertEquals(estimatedSize, deserialized.getEstimatedSize());
  }
}
