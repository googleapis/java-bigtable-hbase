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
import java.util.Collections;
import java.util.List;
import org.apache.hadoop.hbase.mapreduce.TableSnapshotInputFormat;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;

/**
 * A SnapshotInputFormat that randomizes the order of snapshot splits. This is needed for the
 * Bigtable import job to spread the load across all of the Bigtable tablet servers and allows
 * Bigtable to create initial split quicker.
 */
public class ShuffledTableSnapshotInputFormat extends TableSnapshotInputFormat {
  @Override
  public List<InputSplit> getSplits(JobContext job) throws IOException, InterruptedException {
    List<InputSplit> splits = super.getSplits(job);
    Collections.shuffle(splits);
    return splits;
  }
}
