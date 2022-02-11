package com.google.cloud.bigtable.hbase.replication.adapters;

import org.apache.hadoop.hbase.Cell;

import java.util.ArrayList;
/**
 * BigtableWALEntry abstracts minimal functionality from WAL.Entry required for this replication
 * library.
 */
public class BigtableWALEntry {
    private long walWriteTimestamp;
    private ArrayList<Cell> cells;
    private String tableName;

    public BigtableWALEntry(long timeStamp, ArrayList<Cell> cells, String tableName) {
        this.walWriteTimestamp = timeStamp;
        this.cells = cells;
        this.tableName = tableName;
    }

    public ArrayList<Cell> getCells() {
        return this.cells;
    }

    public long getWalWriteTime() {
        return this.walWriteTimestamp;
    }

    public String getTableName() {
        return this.tableName;
    }
}