package com.google.cloud.bigtable.hbase;

import com.google.api.core.InternalApi;
import com.google.cloud.bigtable.data.v2.models.Query;
import org.apache.hadoop.hbase.client.Scan;

@InternalApi
public class BigtableFixedRequestExtendedScan extends Scan {

    private final Query query;

    public BigtableFixedRequestExtendedScan(Query query) {
        this.query = query;
    }

    public Query getQuery() {
        return query;
    }


}
