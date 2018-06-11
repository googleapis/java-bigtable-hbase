/*
 * Copyright 2016 Google Inc. All Rights Reserved.
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

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;

/**
 * An InputFormat randomizes the order of SequenceFile splits. This is needed for the Bigtable
 * import job to spread the load across all of the Bigtable tablet servers.
 * @author igorbernstein2
 * @version $Id: $Id
 */
public class ShuffledSequenceFileInputFormat<K, V> extends SequenceFileInputFormat<K, V> {
  @Override
  public List<InputSplit> getSplits(JobContext job) throws IOException {
    List<InputSplit> splits = super.getSplits(job);
    Collections.shuffle(splits);
    return splits;
  }
}
