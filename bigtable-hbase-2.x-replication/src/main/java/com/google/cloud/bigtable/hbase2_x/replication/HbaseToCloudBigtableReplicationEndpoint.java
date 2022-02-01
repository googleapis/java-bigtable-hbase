package com.google.cloud.bigtable.hbase2_x.replication;

import com.google.cloud.bigtable.hbase.replication.CloudBigtableReplicator;
import com.google.cloud.bigtable.hbase.replication.adapters.BigtableWALEntry;
import org.apache.hadoop.hbase.replication.BaseReplicationEndpoint;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;
import java.util.UUID;

import static java.util.stream.Collectors.groupingBy;


public class HbaseToCloudBigtableReplicationEndpoint extends BaseReplicationEndpoint {
    private static final Logger LOG =
        LoggerFactory.getLogger(HbaseToCloudBigtableReplicationEndpoint.class);

    private final CloudBigtableReplicator cloudBigtableReplicator;
    public HbaseToCloudBigtableReplicationEndpoint() {
        super();
        cloudBigtableReplicator = new CloudBigtableReplicator();
    }

    @Override
    public UUID getPeerUUID() {
        return cloudBigtableReplicator.getPeerUUID(); }

    @Override
    public boolean replicate(ReplicateContext replicateContext) {
        Map<String, List<BigtableWALEntry>> walEntriesByTable =
            replicateContext.getEntries().stream()
            .map(entry -> new BigtableWALEntryImpl(entry).getBigtableWALEntry())
            .collect(groupingBy(e -> e.getTableName()));
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
        cloudBigtableReplicator.start(ctx.getConfiguration(), ctx.getMetrics());
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