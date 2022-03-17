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


import com.google.cloud.bigtable.beam.hbasesnapshots.ImportJobFromHbaseSnapshot;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Function;
import com.google.common.base.Predicate;
import com.google.common.base.Predicates;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.collect.Multimaps;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.*;

<<<<<<< HEAD
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.values.KV;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
=======
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.windowing.PaneInfo;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollectionView;
import org.apache.beam.sdk.values.TupleTag;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.client.Delete;
>>>>>>> origin/main
import org.apache.hadoop.hbase.client.Mutation;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.util.Bytes;
<<<<<<< HEAD
=======
import org.joda.time.Instant;
>>>>>>> origin/main
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A {@link DoFn} function that converts a {@link KV<ImmutableBytesWritable, Result>} in the pipeline input to a {@link
 * KV<ImmutableBytesWritable,Result>} with newly transformed rowkey for output.
 */


public class CustomIndexTableTransform extends DoFn<KV<ImmutableBytesWritable, Result>, Mutation> {
    private static Logger logger = LoggerFactory.getLogger(HBaseResultToMutationFn.class);

    private static final long serialVersionUID = 1L;

    private static final int MAX_CELLS = 100_000 - 1;

    private static final int MAX_VALUE = 999999999; //999,999,999

    private static final String DELIMITER = "#";

    private static final Predicate<Cell> IS_DELETE_MARKER_FILTER =
            new Predicate<Cell>() {
                @Override
                public boolean apply(Cell cell) {
                    return CellUtil.isDelete(cell);
                }
            };

    private static final Function<Cell, String> COLUMN_FAMILY_EXTRACTOR =
            new Function<Cell, String>() {
                @Override
                public String apply(Cell cell) {
                    return Bytes.toString(CellUtil.cloneFamily(cell));
                }
            };

    private static final DataCellPredicateFactory DATA_CELL_PREDICATE_FACTORY =
            new DataCellPredicateFactory();

    private transient boolean isEmptyRowWarned;

    @VisibleForTesting
    static void setLogger(Logger log) {
        logger = log;
    }

    @ProcessElement
    public void processElement(ProcessContext context) throws Exception {
        logger.warn("Starting Custom Index Table Mutation Process");
        indexTableProcess(context);
    }

    /**
     * Transform row key with tenant id expected as string
     * @param currentKey
     * @param tenantId
     * @return
     */
    protected byte[] stringPrefixRowKeyTransform(ImmutableBytesWritable currentKey,
                                                 String tenantId) {

        tenantId = tenantId.concat(DELIMITER);

        return ByteBuffer.allocate(currentKey.get().length + tenantId.getBytes().length)
                .put(ByteBuffer.allocate(tenantId.getBytes().length).put(tenantId.getBytes()).array())
                .put(currentKey.get())
                .array();
    }

    /**
     Modified HBaseResultToMutationFn - takes a row key with a timestamp, duplicates and reverses the timestamp
     **/
    private void indexTableProcess(ProcessContext context) throws IOException {
        logger.info("Starting HBaseResultToMutationFn in single_table_reverse mode.");

        KV<ImmutableBytesWritable, Result> kv = context.element();
        ImportJobFromHbaseSnapshot.ImportOptions.CustomPipelineOptions customOpts = context.getPipelineOptions().as(ImportJobFromHbaseSnapshot.ImportOptions.CustomPipelineOptions.class);
<<<<<<< HEAD

        byte[] rowKey = kv.getKey().get();
        if (customOpts.getTenantId() != null) {
            rowKey = stringPrefixRowKeyTransform(kv.getKey(), customOpts.getTenantId().toString());
        }
        byte[] reversedRowKey = reverseRowKey(rowKey);
=======
        byte[] stringPrefixedRowKey = stringPrefixRowKeyTransform(kv.getKey(), customOpts.getTenantId().toString());
        byte[] reversedRowKey = reverseRowKey(stringPrefixedRowKey);
>>>>>>> origin/main

        List<Cell> newCells = new ArrayList<>();

        List<Cell> cells = checkEmptyRow(kv);
        if (cells.isEmpty()) {
            return;
        }

        // Preprocess delete markers
        if (hasDeleteMarkers(cells)) {
            cells = preprocessDeleteMarkers(cells);
        }
        // Split the row into multiple puts if it exceeds the maximum mutation limit
        Iterator<Cell> cellIt = cells.iterator();

        while (cellIt.hasNext()) {
            Put reversedPut = new Put(reversedRowKey);
            for (int i = 0; i < MAX_CELLS / 2 && cellIt.hasNext(); i++) { // divide max cells by two when performing reverse scans
                Cell originalCell = cellIt.next();

                // reversed
                Cell reversedCell = CellUtil.createCell(
                        reversedRowKey,
                        CellUtil.cloneFamily(originalCell),
                        CellUtil.cloneQualifier(originalCell),
                        originalCell.getTimestamp(),
                        originalCell.getTypeByte(),
<<<<<<< HEAD
                        rowKey); // reversed key : originalKey
=======
                        stringPrefixedRowKey); // reversed key : originalKey
>>>>>>> origin/main
                newCells.add(reversedCell);
                reversedPut.add(reversedCell);
            }
            context.output(reversedPut);
        }
    }


    /**
     * Reverse the last value within a row key
     * @param key
     * @return
     */
    protected byte[] reverseRowKey(byte[] key) {
        String[] originalKeyParts = new String(key, StandardCharsets.UTF_8).split(DELIMITER);
        Integer reversedVal = MAX_VALUE - Integer.parseInt(originalKeyParts[originalKeyParts.length-1]);

        // compile new key
        String newKey = "";
        for (int i = 0; i < originalKeyParts.length - 1; i++) {
            if (i == 1) {
                newKey = newKey.concat("rev" + DELIMITER);
            }
            newKey = newKey.concat(originalKeyParts[i] + DELIMITER);  // msg11#fromNardos#toAdbul#982736578
        }                                                             // tenantId#msg11#fromNardos#toAdbul#982736578
        newKey = newKey.concat(reversedVal.toString());               // tenantId#rev#msg11#fromNardos#toAbdul#17263421

<<<<<<< HEAD
        // for single table, have reversed as prefix rev
=======
        // for single table, have reversed as prefix rev_
>>>>>>> origin/main
        // for double table, reversed as suffix _rev

//    logger.warn("reversedVal:" + reversedVal);
//    logger.warn("newKey: " + newKey);

        return ByteBuffer.allocate(newKey.getBytes().length)
                .put(newKey.getBytes())
                .array();
    }

    private boolean hasDeleteMarkers(List<Cell> cells) {
        for (Cell cell : cells) {
            if (CellUtil.isDelete(cell)) {
                return true;
            }
        }
        return false;
    }

    // Process
    private List<Cell> preprocessDeleteMarkers(List<Cell> cells) {
        List<Cell> resultCells = Lists.newArrayList();

        // Group cells by column family, since DeleteMarkers do not apply across families.
        Map<String, Collection<Cell>> dataCellsByFamilyMap =
                Multimaps.index(
                                Iterables.filter(cells, Predicates.not(IS_DELETE_MARKER_FILTER)),
                                COLUMN_FAMILY_EXTRACTOR)
                        .asMap();
        Map<String, Collection<Cell>> deleteMarkersByFamilyMap =
                Multimaps.index(Iterables.filter(cells, IS_DELETE_MARKER_FILTER), COLUMN_FAMILY_EXTRACTOR)
                        .asMap();
        for (Map.Entry<String, Collection<Cell>> e : dataCellsByFamilyMap.entrySet()) {
            processOneColumnFamily(resultCells, e.getValue(), deleteMarkersByFamilyMap.get(e.getKey()));
        }
        return resultCells;
    }

    private void processOneColumnFamily(
            List<Cell> resultCells, Collection<Cell> dataCells, Collection<Cell> deleteMarkers) {
        if (deleteMarkers == null) {
            // No markers for this column family
            resultCells.addAll(dataCells);
        } else {
            // Build a filter for live data cells that should be sent to bigtable.
            // These are cells not marked by any delete markers in this row/family.
            Predicate<Cell> liveDataCellPredicate =
                    Predicates.not(
                            Predicates.or(
                                    Lists.newArrayList(
                                            Iterables.transform(deleteMarkers, DATA_CELL_PREDICATE_FACTORY))));
            for (Cell cell : dataCells) {
                if (liveDataCellPredicate.apply(cell)) {
                    resultCells.add(cell);
                }
            }
        }
    }

    // Warns about empty row on first occurrence only and replaces a null array with 0-length one.
    private List<Cell> checkEmptyRow(KV<ImmutableBytesWritable, Result> kv) {
        List<Cell> cells = kv.getValue().listCells();
        if (cells == null) {
            cells = Collections.emptyList();
        }
        if (!isEmptyRowWarned && cells.isEmpty()) {
            logger.warn("Encountered empty row. Was input file serialized by HBase 0.94?");
            isEmptyRowWarned = true;
        }
        return cells;
    }


}
