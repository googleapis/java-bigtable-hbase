/*
 * Copyright 2017 Google Inc. All Rights Reserved.
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
package com.google.cloud.bigtable.beam.sequencefiles;

import com.google.common.collect.Lists;
import java.util.List;
import org.apache.beam.sdk.io.DefaultFilenamePolicy;
import org.apache.beam.sdk.io.FileBasedSink.FilenamePolicy;
import org.apache.beam.sdk.io.LocalResources;
import org.apache.beam.sdk.io.Read;
import org.apache.beam.sdk.io.WriteFiles;
import org.apache.beam.sdk.io.fs.ResourceId;
import org.apache.beam.sdk.options.ValueProvider;
import org.apache.beam.sdk.options.ValueProvider.StaticValueProvider;
import org.apache.beam.sdk.testing.NeedsRunner;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.values.KV;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.serializer.WritableSerialization;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.TemporaryFolder;

public class SequenceFileSinkTest {
  @Rule
  public TestPipeline writePipeline = TestPipeline.create();

  @Rule
  public TestPipeline readPipeline = TestPipeline.create();

  @Rule
  public final TemporaryFolder workDir = new TemporaryFolder();

  @Test
  @Category(NeedsRunner.class)
  public void testSeqFileWriteAndRead() throws Throwable {
    List<KV<Text, Text>> data = Lists.newArrayList();
    for(int i=0; i < 100; i++) {
      data.add(KV.of(new Text("key" + i), new Text("value"+i)));
    }

    ValueProvider<ResourceId> output = StaticValueProvider.of(
        LocalResources.fromFile(workDir.getRoot(), true)
    );

    FilenamePolicy filenamePolicy = DefaultFilenamePolicy
        .constructUsingStandardParameters(output, "output", null);

    SequenceFileSink<Text, Text> sink = new SequenceFileSink<>(
        output, filenamePolicy,
        Text.class, WritableSerialization.class,
        Text.class, WritableSerialization.class);

    writePipeline
        .apply(Create.of(data))
        .apply(
            WriteFiles.to(sink)
                .withNumShards(1)
        );

    writePipeline.run().waitUntilFinish();


    SequenceFileSource<Text, Text> source = new SequenceFileSource<>(
        StaticValueProvider.of(workDir.getRoot().toString() + "/*"),
        Text.class, WritableSerialization.class,
        Text.class, WritableSerialization.class,
        SequenceFile.SYNC_INTERVAL
    );
    PAssert.that(
      readPipeline.apply(Read.from(source))
    ).containsInAnyOrder(data);

    readPipeline.run();
  }
}
