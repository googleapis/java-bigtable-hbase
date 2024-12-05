/*
 * Copyright 2022 Google LLC
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
package com.google.cloud.bigtable.mapreduce.validation;

import java.io.OutputStream;
import java.io.PrintStream;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.mapreduce.BigtableSyncTableAccessor;
import org.junit.Assert;
import org.junit.Test;

/** test driver function */
// TODO - parameterize this to run against prod in future
public class TestBigtableSyncTableJob {
  private static final PrintStream NOOP_STREAM =
      new PrintStream(
          new OutputStream() {
            @Override
            public void write(int b) {}
          });

  @Test
  public void testMissingArgs() {
    String[] args = {
      "--sourcezkcluster=hbase-m:2181:/hbase",
      "--targetbigtableproject=prod-app-bigtable-svcs",
      "--targetbigtableinstance=prod-app-events",
      "--targetbigtableappprofile=default",
      "gs://hbase-migration-table1-bucket/hbase-hash-output/",
      "table-source"
    };

    Configuration conf = new Configuration(false);
    BigtableSyncTableJob bigtableSyncTable = createJob(conf);
    boolean isSuccess = bigtableSyncTable.doCommandLine(bigtableSyncTable, args);

    Assert.assertFalse(isSuccess);
  }

  @Test
  public void testArgsHbaseToBigtable() {
    String[] args = {
      "--sourcezkcluster=hbase-m:2181:/hbase",
      "--targetbigtableproject=prod-app-bigtable-svcs",
      "--targetbigtableinstance=prod-app-events",
      "--targetbigtableappprofile=default",
      "gs://hbase-migration-table1-bucket/hbase-hash-output/",
      "table-source",
      "table-target"
    };

    Configuration conf = new Configuration(false);
    BigtableSyncTableJob bigtableSyncTable = createJob(conf);
    boolean isSuccess = bigtableSyncTable.doCommandLine(bigtableSyncTable, args);

    Assert.assertTrue(isSuccess);
    Assert.assertEquals(
        parseKeyValueArg(args[0]), BigtableSyncTableAccessor.getSourceZkCluster(bigtableSyncTable));
    Assert.assertEquals(parseKeyValueArg(args[1]), bigtableSyncTable.getTargetBigtableProjectId());
    Assert.assertEquals(parseKeyValueArg(args[2]), bigtableSyncTable.getTargetBigtableInstance());
    Assert.assertEquals(parseKeyValueArg(args[3]), bigtableSyncTable.getTargetBigtableAppProfile());
  }

  @Test
  public void testArgsBigtableToHbase() {
    String[] args = {
      "--sourcebigtableproject=prod-app-bigtable-svcs",
      "--sourcebigtableinstance=prod-app-events",
      "--sourcebigtableappprofile=default",
      "--targetzkcluster=hbase-m:2181:/hbase",
      "gs://hbase-migration-table1-bucket/bigtable-hash-output/",
      "table-source",
      "table-target"
    };

    Configuration conf = new Configuration(false);
    BigtableSyncTableJob bigtableSyncTable = createJob(conf);
    boolean isSuccess = bigtableSyncTable.doCommandLine(bigtableSyncTable, args);

    Assert.assertTrue(isSuccess);
    Assert.assertEquals(parseKeyValueArg(args[0]), bigtableSyncTable.getSourceBigtableProjectId());
    Assert.assertEquals(parseKeyValueArg(args[1]), bigtableSyncTable.getSourceBigtableInstance());
    Assert.assertEquals(parseKeyValueArg(args[2]), bigtableSyncTable.getSourceBigtableAppProfile());
    Assert.assertEquals(
        parseKeyValueArg(args[3]), BigtableSyncTableAccessor.getTargetZkCluster(bigtableSyncTable));
  }

  @Test
  public void testArgsHBaseToHbase() {
    String[] args = {
      "--sourcezkcluster=hbase-m:2181:/hbase",
      "--targetzkcluster=hbase-m:2181:/hbase",
      "gs://hbase-migration-table1-bucket/bigtable-hash-output/",
      "table-source",
      "table-target"
    };

    Configuration conf = new Configuration(false);
    BigtableSyncTableJob bigtableSyncTable = createJob(conf);
    boolean isSuccess = bigtableSyncTable.doCommandLine(bigtableSyncTable, args);

    Assert.assertTrue(isSuccess);
    Assert.assertEquals(
        parseKeyValueArg(args[0]), BigtableSyncTableAccessor.getSourceZkCluster(bigtableSyncTable));
    Assert.assertEquals(
        parseKeyValueArg(args[1]), BigtableSyncTableAccessor.getTargetZkCluster(bigtableSyncTable));
  }

  @Test
  public void testArgsFailureNoSourceOrTarget() {
    String[] args = {
      "gs://hbase-migration-table1-bucket/bigtable-hash-output/", "table-source", "table-target"
    };

    Configuration conf = new Configuration(false);
    BigtableSyncTableJob bigtableSyncTable = createJob(conf);
    boolean isSuccess = bigtableSyncTable.doCommandLine(bigtableSyncTable, args);

    Assert.assertFalse(isSuccess);
  }

  @Test
  public void testArgsFailureBothSources() {
    String[] args = {
      "--sourcezkcluster=hbase-m:2181:/hbase",
      "--sourcebigtableproject=prod-app-bigtable-svcs",
      "--sourcebigtableinstance=prod-app-events",
      "--sourcebigtableappprofile=default",
      "--targetzkcluster=hbase-m:2181:/hbase",
      "gs://hbase-migration-table1-bucket/bigtable-hash-output/",
      "table-source",
      "table-target"
    };

    Configuration conf = new Configuration(false);
    BigtableSyncTableJob bigtableSyncTable = createJob(conf);
    boolean isSuccess = bigtableSyncTable.doCommandLine(bigtableSyncTable, args);

    Assert.assertFalse(isSuccess);
  }

  @Test
  public void testArgsFailureSourceBigtableMissingProject() {
    String[] args = {
      "--sourcebigtableinstance=prod-app-events",
      "--sourcebigtableappprofile=default",
      "--targetzkcluster=hbase-m:2181:/hbase",
      "gs://hbase-migration-table1-bucket/bigtable-hash-output/",
      "table-source",
      "table-target"
    };

    Configuration conf = new Configuration(false);
    BigtableSyncTableJob bigtableSyncTable = createJob(conf);
    boolean isSuccess = bigtableSyncTable.doCommandLine(bigtableSyncTable, args);

    Assert.assertFalse(isSuccess);
  }

  @Test
  public void testArgsFailureSourceBigtableMissingInstance() {
    String[] args = {
      "--sourcebigtableinstance=prod-app-events",
      "--sourcebigtableappprofile=default",
      "--targetzkcluster=hbase-m:2181:/hbase",
      "gs://hbase-migration-table1-bucket/bigtable-hash-output/",
      "table-source",
      "table-target"
    };

    Configuration conf = new Configuration(false);
    BigtableSyncTableJob bigtableSyncTable = createJob(conf);
    boolean isSuccess = bigtableSyncTable.doCommandLine(bigtableSyncTable, args);

    Assert.assertFalse(isSuccess);
  }

  @Test
  public void testArgsFailureBothTargets() {
    String[] args = {
      "--sourcezkcluster=hbase-m:2181:/hbase",
      "--targetzkcluster=hbase-m:2181:/hbase",
      "--targetbigtableproject=prod-app-bigtable-svcs",
      "--targetbigtableinstance=prod-app-events",
      "--targetbigtableappprofile=default",
      "gs://hbase-migration-table1-bucket/hbase-hash-output/",
      "table-source",
      "table-target"
    };

    Configuration conf = new Configuration(false);
    BigtableSyncTableJob bigtableSyncTable = createJob(conf);
    boolean isSuccess = bigtableSyncTable.doCommandLine(bigtableSyncTable, args);

    Assert.assertFalse(isSuccess);
  }

  @Test
  public void testArgsFailureTargetBigtableMissingProject() {
    String[] args = {
      "--sourcezkcluster=hbase-m:2181:/hbase",
      "--targetbigtableinstance=prod-app-events",
      "--targetbigtableappprofile=default",
      "gs://hbase-migration-table1-bucket/hbase-hash-output/",
      "table-source",
      "table-target"
    };

    Configuration conf = new Configuration(false);
    BigtableSyncTableJob bigtableSyncTable = createJob(conf);
    boolean isSuccess = bigtableSyncTable.doCommandLine(bigtableSyncTable, args);

    Assert.assertFalse(isSuccess);
  }

  @Test
  public void testArgsFailureTargetBigtableMissingInstance() {
    String[] args = {
      "--sourcezkcluster=hbase-m:2181:/hbase",
      "--targetzkcluster=hbase-m:2181:/hbase",
      "--targetbigtableproject=prod-app-bigtable-svcs",
      "--targetbigtableappprofile=default",
      "gs://hbase-migration-table1-bucket/hbase-hash-output/",
      "table-source",
      "table-target"
    };

    Configuration conf = new Configuration(false);
    BigtableSyncTableJob bigtableSyncTable = createJob(conf);
    boolean isSuccess = bigtableSyncTable.doCommandLine(bigtableSyncTable, args);

    Assert.assertFalse(isSuccess);
  }

  private static BigtableSyncTableJob createJob(Configuration conf) {
    BigtableSyncTableJob bigtableSyncTable = new BigtableSyncTableJob(conf);
    bigtableSyncTable.SERR = NOOP_STREAM;
    bigtableSyncTable.SOUT = NOOP_STREAM;
    return bigtableSyncTable;
  }

  private String parseKeyValueArg(String keyValueArg) {
    return keyValueArg.split("=")[1];
  }
}
