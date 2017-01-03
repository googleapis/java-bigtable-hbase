package com.google.cloud.bigtable;

import com.google.bigtable.admin.v2.DropRowRangeRequest;
import com.google.cloud.bigtable.config.BigtableOptions;
import com.google.cloud.bigtable.grpc.BigtableSession;

/**
 * Created by igorbernstein on 12/28/16.
 */
public class ClearTable {
    public static void main(String[] args) throws Exception {
        BigtableOptions options = new BigtableOptions.Builder()
                .setUserAgent("igorbernstein-dev")
                .setProjectId("igorbernstein-dev")
                .setInstanceId("instance1")

                .build();

        try(BigtableSession session = new BigtableSession(options)) {
            session.getTableAdminClient().dropRowRange(
                    DropRowRangeRequest.newBuilder()
                            .setName("projects/igorbernstein-dev/instances/instance1/tables/table1")
                            .setDeleteAllDataFromTable(true)
                            .build()
            );
        }
    }
}
