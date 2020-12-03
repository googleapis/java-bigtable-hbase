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

import com.google.bigtable.repackaged.com.google.bigtable.v2.RowRange;
import com.google.bigtable.repackaged.com.google.cloud.bigtable.grpc.BigtableSession;
import com.google.bigtable.repackaged.com.google.protobuf.ByteString;
import com.google.cloud.bigtable.beam.AbstractCloudBigtableTableDoFn;
import com.google.cloud.bigtable.beam.TemplateUtils;
import com.google.cloud.bigtable.beam.sequencefiles.SyncTableJob.SyncTableOptions;
import com.google.cloud.bigtable.hbase.BigtableExtendedScan;
import java.util.List;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.values.KV;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.HadoopHashTableSource.RangeHash;
import org.apache.hadoop.hbase.mapreduce.HashTable.ResultHasher;
import org.apache.hadoop.hbase.util.Bytes;

/**
 * A {@link DoFn} function that takes a range and hash from HBase and validates the hash from rows
 * read from Bigtable.
 */
public class ComputeHashRangeFromBigtableDoFn
    extends AbstractCloudBigtableTableDoFn<KV<String, Iterable<List<RangeHash>>>, RangeHash> {

  private static final long serialVersionUID = 1L;
  private String tableName = null;

  private transient BigtableSession session;

  public ComputeHashRangeFromBigtableDoFn(SyncTableOptions options) {
    super(TemplateUtils.BuildImportConfig(options));
    this.tableName = options.getBigtableTableId().get();
  }

  private String immutableBytesToString(ImmutableBytesWritable bytes) {
    if (bytes == null) {
      return "";
    }
    return immutableBytesToString(bytes.get());
  }

  private String immutableBytesToString(byte[] bytes) {
    return Bytes.toStringBinary(bytes);
  }

  @StartBundle
  public void start(StartBundleContext context) {
    // TODO: Currently all the batches from a workitem are in single bundle. Not sure how to split
    // it. Having a single bundle per file can create memory pressure, since we are keeping every
    // thing in memory in the buffer.
    DOFN_LOG.error("Starting a new bundle on thread " + Thread.currentThread().getName());
  }

  @ProcessElement
  public void processElement(ProcessContext context) throws Exception {
    // Ideally, we will have only 1 iterable, but iterate just in case.
    for (List<RangeHash> buffer : context.element().getValue()) {
      // logger.error(
      //     "Writing hash pair. Key "
      //         + new String(kv.getKey().get())
      //         + " value: "
      //         + new String(kv.getValue().get()));
      if (buffer.isEmpty()) {
        // No rows found, no point in dwelling here.
        DOFN_LOG.warn("No hashes found in finishBundle, exiting.");
        return;
      }

      // TODO: Extract a function to create scan
      ImmutableBytesWritable rangeStartInclusive =
          new ImmutableBytesWritable(buffer.get(0).startInclusive);
      ImmutableBytesWritable rangeEndExclusive =
          new ImmutableBytesWritable(buffer.get(buffer.size() - 1).endExclusive);

      Table table = getConnection().getTable(TableName.valueOf(tableName));
      ResultHasher resultHasher = new ResultHasher();
      resultHasher.startBatch(rangeStartInclusive);
      BigtableExtendedScan scan = new BigtableExtendedScan();

      // TODO: check for off by 1 errors.
      DOFN_LOG.warn(
          "Initiating a scan ["
              + immutableBytesToString(rangeStartInclusive)
              + " , "
              + immutableBytesToString(rangeEndExclusive)
              + ")");

      RowRange.Builder rangeBuilder = RowRange.newBuilder();
      if (rangeStartInclusive.getLength() > 0) {
        rangeBuilder.setStartKeyClosed(ByteString.copyFrom(rangeStartInclusive.get()));
      }
      if (rangeEndExclusive.getLength() > 0) {
        rangeBuilder.setEndKeyOpen(ByteString.copyFrom(rangeEndExclusive.get()));
      }

      // TODO: We need to handle the timestamp filters and version filters from the HBase scan.
      scan.addRange(rangeBuilder.build());
      ResultScanner scanner = table.getScanner(scan);

      int bufferIndex = 0;
      int numRows = 0;

      // We validated earlier that buffer is not empty.
      RangeHash currentRangeHash = buffer.get(0);

      for (Result result : scanner) {
        numRows++;
        if (numRows % 100000 == 0) {
          // DOFN_LOG.error("Processed " + numRows + " rows ");
        }
        ImmutableBytesWritable rowKey = new ImmutableBytesWritable(result.getRow());
        if (bufferIndex == buffer.size()) {
          DOFN_LOG.error(
              "Buffer reached to end while scan is still active at row :"
                  + new String(result.getRow()));
          DOFN_LOG.error(
              "Affected Range: ["
                  + immutableBytesToString(rangeStartInclusive)
                  + ", "
                  + immutableBytesToString(rangeEndExclusive)
                  + ").");
          // THIS SHOULD NEVER HAPPEN. We are scanning bigtable till the last
          // RangeHash.endKeyExclusive(), so we don't expect the bigtable result to outlast the
          // buffer. No point looking further.
          return;
        }
        if (currentRangeHash.endExclusive.length > 0
            && // For the last EOF range, keep scanning.
            rowKey.compareTo(currentRangeHash.endExclusive) >= 0) {
          ValidateBatchHash(context, resultHasher, numRows, currentRangeHash);
          bufferIndex++;
          if (bufferIndex == buffer.size()) {
            DOFN_LOG.error(
                "Reached the end of buffer with still one row remaining, rem row: "
                    + new String(result.getRow())
                    + " result hash "
                    + currentRangeHash.toString());
          }
          currentRangeHash = buffer.get(bufferIndex);
        }

        // Always Hash the current row.
        resultHasher.hashResult(result);
      }

      // We are finished scanning bigtable but there may still be ranges remaining in the buffer.
      // A happy case is that we have scanned till endKey exclusive and the last range is
      // unverified.
      // verify it.
      ValidateBatchHash(context, resultHasher, numRows, currentRangeHash);
      // Move to next batch
      bufferIndex++;

      // If there are remaining ranges in the buffer they all need to reported as mismatched.
      for (int i = bufferIndex; i < buffer.size(); i++) {
        currentRangeHash = buffer.get(i);
        context.output(currentRangeHash);
        DOFN_LOG.warn(
            "MISMATCH ON "
                + currentRangeHash.toString()
                + " . Processed "
                + numRows
                + " rows for the scan.");
      }
      DOFN_LOG.error(
          "Finishing context by outputting "
              + buffer.size()
              + " keys in range ["
              + ((!buffer.isEmpty())
                  ? immutableBytesToString(buffer.get(0).startInclusive)
                      + ", "
                      + immutableBytesToString(buffer.get(buffer.size() - 1).endExclusive)
                      + "]."
                  : "]."));
    }
  }

  private void ValidateBatchHash(
      ProcessContext context, ResultHasher resultHasher, int numRows, RangeHash currentRangeHash) {
    resultHasher.finishBatch();
    if (!resultHasher.getBatchHash().equals(currentRangeHash.hash)) {
      DOFN_LOG.warn(
          "MISMATCH ON RANGE ["
              + immutableBytesToString(currentRangeHash.startInclusive)
              + ", "
              + immutableBytesToString(currentRangeHash.endExclusive)
              + "). Processed "
              + numRows
              + " rows for the scan.");
      context.output(currentRangeHash);
    } else {
      // Too much logging is throttling logger.
      // DOFN_LOG.warn(
      //     "Hash MATCHED ON RANGE ["
      //         + immutableBytesToString(lastHash.getKey())
      //         + ", "
      //         + immutableBytesToString(buffer.get(bufferIndex).getKey())
      //         + "). Processed "
      //         + numRows
      //         + " rows for the scan.");
    }
    // Start a new batch
    resultHasher.startBatch(new ImmutableBytesWritable(currentRangeHash.endExclusive));
  }
}
