/*
 * Copyright 2017 Google LLC
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

package com.google.cloud.bigtable.beam.sequencefiles;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.SimpleFunction;
import org.apache.beam.sdk.values.KV;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import java.io.IOException;
import java.util.Iterator;
import java.util.List;


/**
 * Get total byte size sum of import job
 */
public class ImportByteSizeSum extends DoFn<KV<ImmutableBytesWritable, Result>, Integer> {

    private static final int MAX_CELLS = 100_000 - 1;

    @ProcessElement
    public void processElement(ProcessContext context) throws IOException {

        KV<ImmutableBytesWritable, Result> kv = context.element();

        List<Cell> cells = kv.getValue().listCells();
        if (cells.isEmpty()) {
            return;
        }

        // Split the row into multiple puts if it exceeds the maximum mutation limit
        Iterator<Cell> cellIt = cells.iterator();

        int totalByteSize = 0;

        while (cellIt.hasNext()) {
            for (int i = 0; i < MAX_CELLS && cellIt.hasNext(); i++) {
                Cell originalCell = cellIt.next();
                totalByteSize = totalByteSize + originalCell.getRowArray().length;
            }
            context.output(totalByteSize);
        }
    }

    /** A SimpleFunction that converts a Count into a printable string. */
    public static class FormatAsTextFn extends SimpleFunction<Integer, String> {
        @Override
        public String apply(Integer input) {
            return "Total Size: " + input.toString();
        }
    }
}