package com.google.cloud.bigtable.hbase.replication;

import com.google.cloud.bigtable.hbase.replication.adapters.BigtableWALEntry;
import org.apache.hadoop.hbase.replication.BaseReplicationEndpoint;
import org.apache.hadoop.hbase.wal.WAL;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
// TODO(remove BaseReplicationEndpoint extension).
public class HbaseToCloudBigtableReplicationEndpoint extends BaseReplicationEndpoint {
    private static final Logger LOG =
        LoggerFactory.getLogger(HbaseToCloudBigtableReplicationEndpoint.class);

    private final CloudBigtableReplicator cloudBigtableReplicator;
    private HBaseMetricsExporter metricsExporter;

    public HbaseToCloudBigtableReplicationEndpoint() {
        super();
        cloudBigtableReplicator = new CloudBigtableReplicator();
        metricsExporter = new HBaseMetricsExporter();
    }

    @Override
    public UUID getPeerUUID() {
        return cloudBigtableReplicator.getPeerUUID(); }

    @Override
    public boolean replicate(ReplicateContext replicateContext) {
        Map<String, List<BigtableWALEntry>> walEntriesByTable = new HashMap<>();
        for (WAL.Entry wal: replicateContext.getEntries()) {
            String tableName = wal.getKey().getTableName().getNameAsString();
            BigtableWALEntry bigtableWALEntry = new BigtableWALEntry(wal.getKey().getWriteTime(), wal.getEdit().getCells());
            if (!walEntriesByTable.containsKey(tableName)) {
                walEntriesByTable.put(tableName, new ArrayList<>());
            }
            walEntriesByTable.get(tableName).add(bigtableWALEntry);
        }
        return cloudBigtableReplicator.replicate(walEntriesByTable);
    }

    @Override
    public void start() {
        startAsync();

    }

    @Override
    public void stop() {
        stopAsync();

    }

    @Override
    protected void doStart() {
        LOG.error(
            "Starting replication to CBT. ", new RuntimeException("Dummy exception for stacktrace."));
        metricsExporter.setMetricsSource(ctx.getMetrics());
        cloudBigtableReplicator.start(ctx.getConfiguration(), metricsExporter);
        notifyStarted();

    }

    @Override
    protected void doStop() {
        LOG.error("Stopping replication to CBT for this EndPoint. ",
            new RuntimeException("Dummy exception for stacktrace"));
        cloudBigtableReplicator.stop();
        notifyStopped();
    }
}