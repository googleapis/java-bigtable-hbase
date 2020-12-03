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
package org.apache.hadoop.hbase.mapreduce;

import java.util.ArrayList;
import java.util.List;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.values.KV;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.mapreduce.HadoopHashTableSource.RangeHash;
import org.apache.hadoop.hbase.util.Bytes;

/**
 * A {@link DoFn} function that takes a range and hash from HBase and generates a batch or work. The
 * key is the start key of the batch and value is the sorted range of RangeHases. This function
 * generates a KV<String, List<RangeHash>> to preserve the sorted nature of RangeHashes. There is no
 * guarantee of preseving the order of RangeHash if this function returns KV<String,RangeHash>.
 *
 * <p>Sorted list of RangeHashes allows the subsequent functions to issue a single scan to bigtable
 * covering all the ranges in the list of RangeHash.
 */
public class GenerateGroupByKeyDoFn extends DoFn<RangeHash, KV<String, List<RangeHash>>> {

  // Batch 50 row ranges in a batch. We may change it in future to be based on the size of a batch.
  // Having smaller batches also helps in validateHash stage as it doesn't emit an output in happy
  // case. Finishing smaller bundles will tell the dataflow service that worker is alive.
  private static final int GROUP_BY_BATCH_SIZE = 50;
  private static final long serialVersionUID = 1L;
  private List<RangeHash> buffer = new ArrayList<>(GROUP_BY_BATCH_SIZE);
  private static final Log LOG = LogFactory.getLog(GenerateGroupByKeyDoFn.class);

  @StartBundle
  public void start(StartBundleContext context) {
    // TODO: Currently all the batches from a workitem are in single bundle. Not sure how to split
    // it. Having a single bundle per file can create memory pressure, since we are keeping every
    // thing in memory in the buffer.
    LOG.error("Starting a new bundle on thread " + Thread.currentThread().getName());
    buffer = new ArrayList<>(GROUP_BY_BATCH_SIZE);
  }

  @ProcessElement
  public void processElement(ProcessContext context) throws Exception {
    buffer.add(context.element());
    // Used to distribute work amongst workers. We want to split a file but want to retain
    // the contiguous nature of scans. So that for each bundle we can do a single bigtable scan.

    if (buffer.size() == GROUP_BY_BATCH_SIZE) {
      // This batch is complete, next item will start a new batch.
      context.output(KV.of(Bytes.toStringBinary(buffer.get(0).startInclusive), buffer));
      buffer = new ArrayList<>(GROUP_BY_BATCH_SIZE);
    }
  }
}
