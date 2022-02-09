package com.google.cloud.bigtable.hbase.replication;

import com.google.cloud.bigtable.hbase.replication.adapters.BigtableWALEntry;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.wal.WAL;

import java.util.ArrayList;

/*
BigtableWALEntryImpl provides BigWALEntry class from hbase1x. WAL.Entry
 */
public class BigtableWALEntryImpl  {
    private long timeStamp;
    private ArrayList<Cell> cells;
    private String tableName;

    public BigtableWALEntryImpl(WAL.Entry entry) {
        // we need List<Cells>, timestamp and name of table.
        this.timeStamp = entry.getKey().getWriteTime();
        this.cells = entry.getEdit().getCells();
        this.tableName = entry.getKey().getTableName().getNameAsString();
    }
    public BigtableWALEntry getBigtableWALEntry() {
        // create BigtableWALEntry which is agnostic of version
        return new BigtableWALEntry(this.timeStamp, this.cells, this.tableName);
    }
}
