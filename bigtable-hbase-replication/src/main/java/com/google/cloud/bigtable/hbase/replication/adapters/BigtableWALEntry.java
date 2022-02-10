package com.google.cloud.bigtable.hbase.replication.adapters;

import org.apache.hadoop.hbase.Cell;

import java.util.ArrayList;
/**
 * BigtableWALEntry abstracts minimal functionality from WAL.Entry required for this replication library.
 */
public class BigtableWALEntry {
    private long timeStamp;
    private ArrayList<Cell> cells;

    public BigtableWALEntry(long timeStamp, ArrayList<Cell> cells) {
        this.timeStamp = timeStamp;
        this.cells = cells;
    }

    public ArrayList<Cell> getCells() {
        return this.cells;
    }

    public long getWalWriteTime() {
        return this.timeStamp;
    }
}