/*
 * Copyright 2015 Google Inc. All Rights Reserved.
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
package com.google.cloud.bigtable.dataflowimport;

import com.google.cloud.bigtable.dataflow.CloudBigtableIO;
import com.google.cloud.bigtable.dataflow.CloudBigtableTableConfiguration;
import com.google.cloud.bigtable.dataflow.coders.HBaseResultCoder;
import com.google.cloud.dataflow.sdk.Pipeline;
import com.google.cloud.dataflow.sdk.coders.KvCoder;
import com.google.cloud.dataflow.sdk.io.BoundedSource;
import com.google.cloud.dataflow.sdk.io.Read;
import com.google.cloud.dataflow.sdk.options.PipelineOptionsFactory;
import com.google.cloud.dataflow.sdk.runners.DirectPipelineRunner;
import com.google.cloud.dataflow.sdk.transforms.PTransform;
import com.google.cloud.dataflow.sdk.transforms.ParDo;
import com.google.cloud.dataflow.sdk.values.KV;
import com.google.cloud.dataflow.sdk.values.PCollection;
import com.google.cloud.hadoop.fs.gcs.GoogleHadoopFS;
import com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Strings;
import com.google.common.collect.ImmutableMap;

import org.apache.hadoop.hbase.client.Mutation;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.ResultSerialization;
import org.apache.hadoop.io.serializer.WritableSerialization;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;

import java.util.Map;

/**
 * <p>
 * {@link com.google.cloud.dataflow.sdk.io.Source} and
 * {@link com.google.cloud.dataflow.sdk.transforms.PTransform}s for importing HBase Sequence Files
 * into <a href="https://cloud.google.com/bigtable/">Google Cloud Bigtable</a>.
 * <p>
 * User of this class should provide dependency for
 * {@code org.apache.hadoop.hbase.mapreduce.ResultSerialization}, which may be found in
 * hbase-server jars.
 *
 * The following code snippets sets up and runs an import pipeline:
 *
 * <pre>
 * {@code
 *   HBaseImportOptions options =
 *       PipelineOptionsFactory.fromArgs(args).withValidation().as(HBaseImportOptions.class);
 *   Pipeline p = CloudBigtableIO.initializeForWrite(Pipeline.create(options));
 *   p
 *       .apply("ReadSequenceFile", Read.from(HBaseImportIO.createSource(options)))
 *       .apply("ConvertResultToMutations", HBaseImportIO.transformToMutations())
 *       .apply("WriteToTable", CloudBigtableIO.writeToTable(
 *           CloudBigtableTableConfiguration.fromCBTOptions(options)));
 *   p.run();
 * }
 * </pre>
 *
 * @author sduskis
 * @version $Id: $Id
 */
public class HBaseImportIO {
  // Needed for HBase 0.94 format. Copied from ResultSerialization.IMPORT_FORMAT_VER.
  @VisibleForTesting
  static final String IMPORT_FORMAT_VER = "hbase.import.version";
  @VisibleForTesting
  static final String VERSION_094_STRING = "0.94";
  @VisibleForTesting
  static final String IO_SERIALIZATIONS = "io.serializations";
  @VisibleForTesting
  static final String FS_GS_IMPL = "fs.gs.impl";
  @VisibleForTesting
  static final String FS_ABSTRACT_FILESYSTEM_GS_IMPL = "fs.AbstractFileSystem.gs.impl";
  @VisibleForTesting
  static final String FS_GS_PROJECT_ID = "fs.gs.project.id";
  /**
   * Constant properties needed by input file readers.The {@code IO_SERIALIZATION} property
   * specifies the classes that can deserialize objects in HBase Sequence Files and is always
   * needed. The {@code FS_GS_IMPL} and {@code FS_ABSTRACT_FILESYSTEM_GS_IMPL} specify
   * implementation of the "gs://" file system. They are needed if the input files are on GCS,
   * and do no harm if the files are not.
   */
  @VisibleForTesting
  static final Map<String, String> CONST_FILE_READER_PROPERTIES = ImmutableMap.of(
      IO_SERIALIZATIONS,
          WritableSerialization.class.getName() + "," + ResultSerialization.class.getName(),
      FS_GS_IMPL, GoogleHadoopFileSystem.class.getName(),
      FS_ABSTRACT_FILESYSTEM_GS_IMPL, GoogleHadoopFS.class.getName());

  /**
   * Returns a {@link com.google.cloud.dataflow.sdk.io.BoundedSource} from an HBase Sequence File for an import pipeline.
   *
   * @param options a {@link com.google.cloud.bigtable.dataflowimport.HBaseImportOptions} object.
   * @return a {@link com.google.cloud.dataflow.sdk.io.BoundedSource} object.
   */
  @SuppressWarnings("unchecked")
  public static BoundedSource<
      KV<ImmutableBytesWritable, Result>> createSource(HBaseImportOptions options) {
    // Tell HadoopFileSource not to access the input files before job is staged on the cloud.
    // See HadoopFileSource.setIsRemoteFileFromLaunchSite() for detailed explanation. This does
    // not affect program correctness but improves user experience.
    HadoopFileSource.setIsRemoteFileFromLaunchSite(
        options.getRunner() != DirectPipelineRunner.class
        && options.getFilePattern().startsWith("gs://"));
    return HadoopFileSource.from(
        options.getFilePattern(),
        SequenceFileInputFormat.class,
        ImmutableBytesWritable.class,
        Result.class,
      KvCoder.of(new WritableCoder<>(ImmutableBytesWritable.class),
        new HBaseResultCoder()),
        createSerializationProperties(options));
  }

  /**
   * Returns a {@link com.google.cloud.dataflow.sdk.transforms.PTransform} that converts {@link org.apache.hadoop.hbase.client.Result}s into
   * {@link org.apache.hadoop.hbase.client.Mutation}s.
   *
   * @return a {@link com.google.cloud.dataflow.sdk.transforms.PTransform} object.
   */
  public static PTransform<
      PCollection<? extends KV<ImmutableBytesWritable, Result>>,
      PCollection<Mutation>> transformToMutations() {
    return ParDo.of(new HBaseResultToMutationFn());
  }

  private static ImmutableMap<String, String> createSerializationProperties(
      HBaseImportOptions options) {
    ImmutableMap.Builder<String, String> builder = ImmutableMap.builder();
    builder.putAll(CONST_FILE_READER_PROPERTIES);

    if (!Strings.isNullOrEmpty(options.getProject())) {
      builder.put(FS_GS_PROJECT_ID, options.getProject());
    }
    if (options.isHBase094DataFormat()) {
      builder.put(IMPORT_FORMAT_VER, VERSION_094_STRING);
    }
    return builder.build();
  }

  public static void main(String[] args) throws Exception {
    HBaseImportOptions options =
        PipelineOptionsFactory.fromArgs(args).withValidation().as(HBaseImportOptions.class);
    Pipeline p = CloudBigtableIO.initializeForWrite(Pipeline.create(options));
    p
        .apply("ReadSequenceFile", Read.from(HBaseImportIO.createSource(options)))
        .apply("ConvertResultToMutations", HBaseImportIO.transformToMutations())
        .apply("WriteToTable", CloudBigtableIO.writeToTable(
            CloudBigtableTableConfiguration.fromCBTOptions(options)));
    p.run();
  }
}
