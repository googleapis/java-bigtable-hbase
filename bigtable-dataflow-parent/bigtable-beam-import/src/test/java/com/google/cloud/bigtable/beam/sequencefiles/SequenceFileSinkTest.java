package com.google.cloud.bigtable.beam.sequencefiles;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import org.apache.beam.runners.direct.DirectOptions;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.DefaultFilenamePolicy;
import org.apache.beam.sdk.io.FileBasedSink.FilenamePolicy;
import org.apache.beam.sdk.io.LocalResources;
import org.apache.beam.sdk.io.WriteFiles;
import org.apache.beam.sdk.io.fs.ResourceId;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptions.DirectRunner;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.options.ValueProvider;
import org.apache.beam.sdk.options.ValueProvider.StaticValueProvider;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.SimpleFunction;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.SequenceFile.Reader;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.serializer.Serialization;
import org.apache.hadoop.io.serializer.WritableSerialization;
import org.junit.Test;

/**
 * Created by igorbernstein on 6/28/17.
 */
public class SequenceFileSinkTest {

  @Test
  public void moo() throws IOException {
    PipelineOptions opts = PipelineOptionsFactory.as(DirectOptions.class);
    Pipeline pipeline = Pipeline.create();

    ValueProvider<ResourceId> output = StaticValueProvider.of(
        LocalResources.fromString("/tmp/moo", true)
    );

    FilenamePolicy filenamePolicy = DefaultFilenamePolicy
        .constructUsingStandardParameters(output, null, null);

    List<Class<? extends Serialization<?>>> serializations = Arrays.<Class<? extends Serialization<?>>>asList(
        ResultSerialization.class, WritableSerialization.class
    );

    List<KV<Text, Text>> data = Arrays.asList(
        KV.of(new Text("one"), new Text("one1")),
        KV.of(new Text("two"), new Text("two2")),
        KV.of(new Text("three"), new Text("three3")),
        KV.of(new Text("three"), new Text("three3")),
        KV.of(new Text("three"), new Text("three3"))
    );

    SequenceFileSink<Text, Text> sink = new SequenceFileSink<>(output, filenamePolicy, Text.class,
        Text.class, serializations);

    pipeline
        .apply(Create.of(data))
        .apply(WriteFiles.to(sink));

    pipeline.run();

    Configuration config = new Configuration(false);
    try (Reader reader = new Reader(config,
        Reader.file(new Path("/tmp/moo/-00000-of-00001"))
    )) {

      reader.sync(0);
      Text key = new Text();
      Text value = new Text();

      while(reader.next(key)) {
        reader.getCurrentValue(value);
        System.out.println("key: " + key.toString() + ", value: " + value.toString());
      }
    }

  }
}
