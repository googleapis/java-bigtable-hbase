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
package com.google.cloud.bigtable.mapreduce.hbasesnapshots;

import static com.google.cloud.bigtable.mapreduce.hbasesnapshots.ImportJobCommon.SNAPSHOTNAME_KEY;
import static com.google.cloud.bigtable.mapreduce.hbasesnapshots.ImportJobCommon.SNAPSHOT_RESTOREDIR_KEY;

import com.google.cloud.bigtable.hbase.BigtableConfiguration;
import com.google.cloud.bigtable.hbase.BigtableOptionsFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CommonConfigurationKeysPublic;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.client.ClusterConnection;
import org.apache.hadoop.hbase.mapreduce.TableOutputFormat;
import org.junit.Assert;
import org.junit.Test;

/** test driver function */
// TODO - parameterize this to run against prod in future
public class TestImportHBaseSnapshotJob {

  @Test
  public void testSetConfFromArgs() {
    Configuration conf = new Configuration(false);
    conf.set(BigtableOptionsFactory.PROJECT_ID_KEY, "test-proj-id");
    conf.set(BigtableOptionsFactory.INSTANCE_ID_KEY, "test-instance-id");
    String[] args = {
      "table1-snapshot",
      "gs://hbase-migration-table1-bucket/export/table1-snapshot",
      "table1",
      "gs://hbase-migration-table1-bucket/export/table1-restore"
    };

    ImportHBaseSnapshotJob.setConfFromArgs(conf, args);

    Assert.assertEquals(args[0], conf.get(SNAPSHOTNAME_KEY));
    Assert.assertEquals(args[1], conf.get(HConstants.HBASE_DIR));
    Assert.assertEquals(args[2], conf.get(TableOutputFormat.OUTPUT_TABLE));
    Assert.assertEquals(args[3], conf.get(SNAPSHOT_RESTOREDIR_KEY));

    Assert.assertEquals(
        conf.get(HConstants.HBASE_DIR),
        conf.get(CommonConfigurationKeysPublic.FS_DEFAULT_NAME_KEY));
    Assert.assertEquals(
        BigtableConfiguration.getConnectionClass().getName(),
        conf.get(ClusterConnection.HBASE_CLIENT_CONNECTION_IMPL));
  }
}
