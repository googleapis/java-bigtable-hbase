package com.google.cloud.bigtable.hbase.replication.adapters;

/*
HbaseBigtableWALEntryAdaptor provides BigtableWALEntry adaptor for any WAL entry
 */
public interface HbaseBigtableWALEntryAdaptor {
    public BigtableWALEntry getBigtableWALEntry();
}
