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
package com.google.cloud.bigtable.mapreduce;

import com.google.cloud.bigtable.mapreduce.hbasesnapshots.ImportHBaseSnapshotJob;
import com.google.cloud.bigtable.mapreduce.validation.BigtableSyncTableJob;
import org.apache.hadoop.classification.InterfaceStability.Evolving;
import org.apache.hadoop.hbase.mapreduce.HashTable;
import org.apache.hadoop.util.ProgramDriver;

/** Driver for bigtable mapreduce jobs. Select which to run by passing name of job to this main. */
@Evolving
public class Driver {

  /**
   * main.
   *
   * @param args an array of {@link java.lang.String} objects.
   */
  public static void main(String[] args) {
    ProgramDriver programDriver = new ProgramDriver();
    int exitCode = -1;
    try {
      programDriver.addClass(
          "export-table",
          Export.class,
          "A map/reduce program that exports a table to sequencefiles.");
      programDriver.addClass(
          "import-table",
          Import.class,
          "A map/reduce program that imports sequencefiles to a table.");
      programDriver.addClass(
          "import-snapshot",
          ImportHBaseSnapshotJob.class,
          "A map/reduce program that imports an hbase snapshot to a table.");
      programDriver.addClass(
          "hash-table",
          HashTable.class,
          "A map/reduce program that computes hashes on source and outputs to filesystem (or cloud storage).");
      programDriver.addClass(
          "sync-table",
          BigtableSyncTableJob.class,
          "A map/reduce program that computes hashes on target and compares with hashes from source.");
      programDriver.driver(args);
      exitCode = programDriver.run(args);
    } catch (Throwable e) {
      e.printStackTrace();
    }
    System.exit(exitCode);
  }
}
