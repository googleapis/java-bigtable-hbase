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
package com.google.cloud.bigtable.beam.validation;

import com.google.bigtable.repackaged.com.google.bigtable.v2.RowRange;
import com.google.bigtable.repackaged.com.google.cloud.bigtable.grpc.BigtableSession;
import com.google.bigtable.repackaged.com.google.protobuf.ByteString;
import com.google.cloud.bigtable.beam.AbstractCloudBigtableTableDoFn;
import com.google.cloud.bigtable.beam.TemplateUtils;
import com.google.cloud.bigtable.beam.validation.HadoopHashTableSource.RangeHash;
import com.google.cloud.bigtable.beam.validation.SyncTableJob.SyncTableOptions;
import com.google.cloud.bigtable.hbase.BigtableExtendedScan;
import java.io.IOException;
import java.util.Iterator;
import java.util.List;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.values.KV;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.BigtableTableHashAccessor.BigtableResultHasher;
import org.apache.hadoop.hbase.util.Bytes;

/**
 * A {@link DoFn} that takes a row range and hash from HBase and validates the hash from rows read
 * from Cloud Bigtable.
 */
public class ComputeAndValidaeHashFromBigtableDoFn
    extends AbstractCloudBigtableTableDoFn<KV<String, Iterable<List<RangeHash>>>, RangeHash> {

  private static final long serialVersionUID = 1L;
  private String tableName;

  private transient BigtableSession session;

  public ComputeAndValidaeHashFromBigtableDoFn(SyncTableOptions options) {
    // TODO Remove the dependency on import options after ImportFromSnapshot is committed.
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
    // Figure out a way to split a single file between different bundles. HashBasedSource creates
    // a single workitem per file, and 1 workitem is held in a single bundle. Granular workitems
    // will reduce memory pressure from the worker host.
    DOFN_LOG.debug("Starting a new bundle on thread " + Thread.currentThread().getName());
  }

  @ProcessElement
  public void processElement(ProcessContext context) throws Exception {
    // GenearteGroupByKeyDoFn generates only 1 item per key, but iterate just in case.
    for (List<RangeHash> buffer : context.element().getValue()) {
      if (buffer.isEmpty()) {
        // No rows found, return;
        return;
      }

      byte[] rangeStartInclusive = buffer.get(0).startInclusive;
      byte[] rangeEndExclusive = buffer.get(buffer.size() - 1).endExclusive;

      BigtableResultHasher resultHasher = new BigtableResultHasher();
      resultHasher.startBatch(new ImmutableBytesWritable(rangeStartInclusive));

      // Since all the row-ranges are sorted in HashTable's data files, 1 big scan can be used
      // to read all the row ranges. Parallelism is achieved by splitting the HashTable's data
      // files into smaller bundle of row-ranges by GroupBy.
      ResultScanner scanner = createBigtableScan(rangeStartInclusive, rangeEndExclusive);

      int bufferIndex = 0;
      // TODO remove bufferIndex and use the iterator
      Iterator<RangeHash> bufferIter = buffer.iterator();
      int numRows = 0;

      RangeHash currentRangeHash = buffer.get(0);

      // Process each row and validate hashes
      for (Result result : scanner) {
        numRows++;
        if (numRows % 100_000 == 0) {
          // Heartbeat in logs in case a large scan gets hung.
          DOFN_LOG.trace("Processed " + numRows + " rows ");
        }

        ImmutableBytesWritable rowKey = new ImmutableBytesWritable(result.getRow());
        if (currentRangeHash.endExclusive.length > 0 // For the last EOF range, keep scanning.
            && rowKey.compareTo(currentRangeHash.endExclusive) >= 0) {
          ValidateBatchHash(context, resultHasher, currentRangeHash);
          bufferIndex++;
          if (bufferIndex == buffer.size()) {
            // THIS SHOULD NEVER HAPPEN. Bigtable is being scanned till the last
            // RangeHash.endKeyExclusive(), so bigtable's result should not outlast the
            // buffer.
            throw new IllegalStateException(
                "Buffer reached to end while scan is still active at row :"
                    + immutableBytesToString(result.getRow())
                    + ". Affected Range: ["
                    + immutableBytesToString(rangeStartInclusive)
                    + ", "
                    + immutableBytesToString(rangeEndExclusive)
                    + ").");
          }
          currentRangeHash = buffer.get(bufferIndex);
        }

        // Always Hash the current row.
        resultHasher.hashResult(result);
      }

      // Bigtable scan is finished at this point and buffer may contain additional row ranges.
      // Last range will always be unverified as the range end is exclusive and
      // currentRow > rangeEndExclusive will never by true. Verify the last range.
      ValidateBatchHash(context, resultHasher, currentRangeHash);
      // Move to next batch
      bufferIndex++;

      // If there are remaining ranges in the buffer they all need to reported as mismatched as
      // there is nothing in Cloud Bigtable for those row ranges.
      for (int i = bufferIndex; i < buffer.size(); i++) {
        currentRangeHash = buffer.get(i);
        context.output(currentRangeHash);
        DOFN_LOG.info(
            "MISMATCH ON "
                + currentRangeHash.toString()
                + " . Processed "
                + numRows
                + " rows for the scan.");
      }
      DOFN_LOG.debug(
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

  private ResultScanner createBigtableScan(byte[] startKeyInclusive, byte[] endKeyExclusive)
      throws IOException {
    Table table = getConnection().getTable(TableName.valueOf(tableName));
    BigtableExtendedScan scan = new BigtableExtendedScan();

    // TODO: check for off by 1 errors.
    DOFN_LOG.debug(
        "Initiating a scan ["
            + immutableBytesToString(startKeyInclusive)
            + " , "
            + immutableBytesToString(endKeyExclusive)
            + ")");

    RowRange.Builder rangeBuilder = RowRange.newBuilder();
    if (startKeyInclusive.length > 0) {
      rangeBuilder.setStartKeyClosed(ByteString.copyFrom(startKeyInclusive));
    }
    if (endKeyExclusive.length > 0) {
      rangeBuilder.setEndKeyOpen(ByteString.copyFrom(endKeyExclusive));
    }

    // TODO: We need to handle the timestamp filters and version filters from the HBase scan.
    scan.addRange(rangeBuilder.build());
    return table.getScanner(scan);
  }

  private void ValidateBatchHash(
      ProcessContext context, BigtableResultHasher resultHasher, RangeHash currentRangeHash) {
    resultHasher.finishBatch();
    if (!resultHasher.getBatchHash().equals(currentRangeHash.hash)) {
      DOFN_LOG.info(
          "MISMATCH ON RANGE ["
              + immutableBytesToString(currentRangeHash.startInclusive)
              + ", "
              + immutableBytesToString(currentRangeHash.endExclusive)
              + ").");
      context.output(currentRangeHash);
    }
    // Start a new batch
    resultHasher.startBatch(new ImmutableBytesWritable(currentRangeHash.endExclusive));
  }
}
