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

import com.google.cloud.bigtable.beam.sequencefiles.ImportJob.ImportOptions;
import com.google.cloud.bigtable.hbase.BigtableConfiguration;
import com.google.common.base.Throwables;
import java.io.File;
import java.net.URISyntaxException;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import javax.annotation.Nullable;
import org.apache.beam.sdk.io.BoundedSource.BoundedReader;
import org.apache.beam.sdk.io.FileBasedSource;
import org.apache.beam.sdk.io.FileSystems;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.values.KV;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.io.serializer.WritableSerialization;

/**
 * Entry point for import/export job submission.
 *
 * @author igorbernstein2
 */
public class Main {
  public static void main(String[] args) throws Exception {
    if (args.length < 1) {
      usage();
      System.exit(1);
    }

    String[] subArgs = Arrays.copyOfRange(args, 1, args.length);

    switch (args[0]) {
      case "export":
        ExportJob.main(subArgs);
        break;
      case "import":
        ImportJob.main(subArgs);
        break;
      case "create-table":
        CreateTableHelper.main(subArgs);
        break;
      default:
        usage();
        System.exit(1);
    }
  }


  private static void usage() {
    String jarName;

    try {
      jarName = new File(Main.class.getProtectionDomain().getCodeSource().getLocation().toURI().getPath()).getName();
    } catch (URISyntaxException e) {
      jarName = "<jar>";
    }

    System.out.printf(
        "java -jar %s <action> <action_params>\n"
            + "Where <action> can be 'export' or 'import'. To get further help, run: \n"
            + "java -jar %s <action> --help\n",
        jarName,
        jarName
    );
  }
}
