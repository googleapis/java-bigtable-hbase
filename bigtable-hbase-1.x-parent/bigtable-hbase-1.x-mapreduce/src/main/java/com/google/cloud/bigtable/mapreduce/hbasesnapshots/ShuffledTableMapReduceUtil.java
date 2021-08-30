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

import java.io.IOException;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.hbase.mapreduce.TableSnapshotInputFormat;
import org.apache.hadoop.hbase.util.RegionSplitter;
import org.apache.hadoop.mapreduce.Job;

/** Utility for {@link TableMapper} for shuffled snapshot input format */
public class ShuffledTableMapReduceUtil extends TableMapReduceUtil {

  /**
   * Sets up the job for reading from a table snapshot. It bypasses hbase servers and read directly
   * from snapshot files.
   *
   * @param snapshotName The name of the snapshot (of a table) to read from.
   * @param scan The scan instance with the columns, time range etc.
   * @param mapper The mapper class to use.
   * @param outputKeyClass The class of the output key.
   * @param outputValueClass The class of the output value.
   * @param job The current job to adjust. Make sure the passed job is carrying all necessary HBase
   *     configuration.
   * @param addDependencyJars upload HBase jars and jars for any of the configured job classes via
   *     the distributed cache (tmpjars).
   * @param tmpRestoreDir a temporary directory to copy the snapshot files into. Current user should
   *     have write permissions to this directory, and this should not be a subdirectory of rootdir.
   *     After the job is finished, restore directory can be deleted.
   * @param splitAlgo algorithm to split
   * @param numSplitsPerRegion how many input splits to generate per one region
   * @throws IOException When setting up the details fails.
   * @see TableSnapshotInputFormat
   */
  public static void initTableSnapshotMapperJob(
      String snapshotName,
      Scan scan,
      Class<? extends TableMapper> mapper,
      Class<?> outputKeyClass,
      Class<?> outputValueClass,
      Job job,
      boolean addDependencyJars,
      Path tmpRestoreDir,
      RegionSplitter.SplitAlgorithm splitAlgo,
      int numSplitsPerRegion)
      throws IOException {
    ShuffledTableSnapshotInputFormat.setInput(
        job, snapshotName, tmpRestoreDir, splitAlgo, numSplitsPerRegion);
    initTableMapperJob(
        snapshotName,
        scan,
        mapper,
        outputKeyClass,
        outputValueClass,
        job,
        addDependencyJars,
        false,
        ShuffledTableSnapshotInputFormat.class);
    resetCacheConfig(job.getConfiguration());
  }
}
