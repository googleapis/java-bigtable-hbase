/*
 * Copyright 2021 Google LLC
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

import com.google.bigtable.repackaged.com.google.api.core.InternalApi;
import com.google.bigtable.repackaged.com.google.api.core.InternalExtensionOnly;
import com.google.cloud.bigtable.beam.hbasesnapshots.ImportJobFromHbaseSnapshot;
import com.google.cloud.bigtable.beam.sequencefiles.CreateTableHelper;
import com.google.cloud.bigtable.beam.sequencefiles.ExportJob;
import com.google.cloud.bigtable.beam.sequencefiles.ImportJob;
import com.google.cloud.bigtable.beam.validation.SyncTableJob;
import java.io.File;
import java.net.URISyntaxException;
import java.util.Arrays;

/** Entry point for create-table/import/export job submission. */
@InternalExtensionOnly
public final class Main {
  /** For internal use only - public for technical reasons. */
  @InternalApi("For internal usage only")
  public Main() {}

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
      case "importsnapshot":
        ImportJobFromHbaseSnapshot.main(subArgs);
        break;
      case "create-table":
        CreateTableHelper.main(subArgs);
        break;
      case "sync-table":
        SyncTableJob.main(subArgs);
        break;
      default:
        usage();
        System.exit(1);
    }
  }

  private static void usage() {
    String jarName;

    try {
      jarName =
          new File(Main.class.getProtectionDomain().getCodeSource().getLocation().toURI().getPath())
              .getName();
    } catch (URISyntaxException e) {
      jarName = "<jar>";
    }

    System.out.printf(
        "java -jar %s <action> <action_params>\n"
            + "Where <action> can be 'export', 'import' , 'importsnapshot' or 'create-table'. To get further help, run: \n"
            + "java -jar %s <action> --help\n",
        jarName, jarName);
  }
}
