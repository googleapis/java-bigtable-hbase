# HBase-Bigtable Replication

## Overview

HBase
provides [async replication](https://hbase.apache.org/book.html#_cluster_replication)
between clusters for various use cases like disaster recovery, data aggregation
etc. Bigtable HBase replication library enables Cloud Bigtable to be added as
HBase cluster replication sink. HBase to Cloud Bigtable replication enables
customers to keep Cloud Bigtable up to date with the mutations happening on
their HBase cluster. This feature enables near zero downtime migrations from
HBase to Cloud Bigtable.

Replication between HBase and Cloud Bigtable will be eventually consistent. This
is a result of the async nature of HBase replication. Cloud Bigtable HBase
replication library will guarantee the same ordering guarantee as HBase.

Cloud Bigtable HBase Replication Library is deployed to HBase region servers.
This jar file contains a replication endpoint responsible for replicating
mutations to Cloud Bigtable. Similar to HBase, destination Cloud Bigtable
cluster should have all the resources (table, column families) created before
enabling replication. You can
use [HBase schema translator](https://cloud.google.com/blog/products/databases/create-bigtable-tables-from-existing-apache-hbase-tables)
for creating pre-split Cloud Bigtable tables.

The service account running the replication library should be assigned an IAM
role
of [roles/bigtable.user](https://cloud.google.com/bigtable/docs/access-control#roles)
. Please
visit [Cloud Bigtable documentation](https://cloud.google.com/bigtable/docs/authentication)
to configure authentication. The library issues mutateRows RPCs.

## Near zero downtime migration from HBase to Cloud Bigtable

CBT is a natural destination for HBase workloads as it is a managed service
compatible with the HBase API. Customers running business critical applications
want to migrate to CBT without taking extended downtime of the applications. CBT
HBase library is a critical component of such near zero downtime migrations.

HBase to Cloud Bigtable replication enables users to keep their Cloud Bigtable
in sync with the production HBase cluster without taking a downtime. Adding
Cloud Bigtable as an HBase replica guarantees that mutations are applied to
Cloud Bigtable in the same order as on HBase. This is a preferred method for the
“dual write” step of near zero-downtime migrations because it can guarantee
ordering and tolerate CBT unavailability without data loss.

Near zero downtime database migration is a multi step procedure. HBase
replication does not support backfilling of existing data. Hence, CBT HBase
replication library only streams “current changes”. Users have to
use [offline migration tools](https://cloud.google.com/architecture/hadoop/hadoop-gcp-migration-data-hbase-to-bigtable)
to copy existing data. In order to avoid any race conditions between backfill
process and replication writes, users should pause replication before starting
backfill job, similar to enabling replication for an HBase table with existing
data. Near zero downtime migrations include the following steps:

1. Install HBase to Cloud Bigtable library on the HBase master and region
   servers
2. Configure Cloud
   Bigtable [authentication](https://cloud.google.com/bigtable/docs/authentication)
3. Update hbase-site.xml with destination cloud bigtable project id, instance id
   and service account json file.
4. Add a CBT replication peer in HBase. On HBase shell
   execute `add_peer '2', ENDPOINT_CLASSNAME => 'com.google.cloud.bigtable.hbase2_x.replication.HbaseToCloudBigtableReplicationEndpoint'`
   . Please use endpoint
   class `com.google.cloud.bigtable.hbase1_x.replication.HbaseToCloudBigtableReplicationEndpoint`
   for HBase 1.x clusters. Use add_peer options to enable replication for select
   tables.
5. Immediately disable the CBT replication peer, this allows WAL logs to
   accumulate on HDFS. On HBase shell execute:  `disable_peer '2'`
6. Check the replicated tables by executing `list_replicated_tables` and enable
   table level replication by executing `enable_table_replication "table_name"`
7. Copy the existing data
   using [offline migration tooling](https://cloud.google.com/architecture/hadoop/hadoop-gcp-migration-data-hbase-to-bigtable)
8. After all the data is copied (and verified), enable CBT replication peer. On
   HBase shell execute `enable_peer '2'`
9. Eventually, replication will catch up and the 2 databases will be in sync. On
   HBase shell execute `status 'replication'` to check status of replication for
   peer ("2" in this example)
10. Run validation steps to ensure compatibility and performance on CBT
11. Once ready, switch over to CBT
    1. Turn down applications writing to HBase
    2. Wait for replication to catch up (This should be fast in the absence of
       new writes)
    3. Turn on the applications that write directly to CBT.
12. Deprecate the HBase cluster

## Prerequisites

- HBase cluster is configured with the setting `hbase.replication` to `true` in
  hbase-site.xml
- Cloud Bigtable instance is created
- Cloud Bigtable instance has all the tables with all column families
- Cloud Bigtable authentication is configured

## HBase configuration

Have the below properties set in `hbase-site.xml` and add it to the HBase
classpath.

```
<property> 
    <name>google.bigtable.project.id</name>
    <value>PROJECT_KEY</value>
    <description>
       Cloud Bigtable Project ID
    </description>
</property>
<property>
    <name>google.bigtable.instance.id</name>
    <value>INSTANCE_KEY</value>
    <description>
       Cloud Bigtable Instance ID
    </description>
</property>
```

We recommend specifying a single-cluster
routing [application profile](https://cloud.google.com/bigtable/docs/app-profiles#routing)
by setting config key
`google.bigtable.app_profile.id`. A single-cluster routing application profile
preserves order of mutations between HBase and Cloud Bigtable.

Next, you should configure Cloud Bigtable authentication. Create a service
account and download a json file as shown
[here](https://cloud.google.com/docs/authentication/production#create_service_account)
. Assign the
role [roles/bigtable.user](https://cloud.google.com/bigtable/docs/access-control#roles)
to the newly created service account to grant it write permissions to Cloud
Bigtable. Pass the json file to Cloud Bigtable client by adding the following
to `hbase-site.xml` file.

```
<property> 
    <name>google.bigtable.auth.json.keyfile</name>
    <value>/path/to/downloaded/json/file</value>
    <description>
        Service account JSON file to connect to Cloud Bigtable
    </description>
</property>
```

Please refer
to [HBaseToCloudBigtableReplicationConfiguration](bigtable-hbase-replication-core/src/main/java/com/google/cloud/bigtable/hbase/replication/configuration/HBaseToCloudBigtableReplicationConfiguration.java)
for other properties that can be set.

### Bidirectional Replication with Cloud Bigtable

To enable bidirectional replication support with Cloud Bigtable, add the following property to `hbase-site.xml`.
```
<property>
    <name>google.bigtable.replication.enable_bidirectional_replication</name>
    <value>true</value>
</property>
```

This feature filters out mutations replicated to Hbase from Cloud Bigtable change streams with the [Bigtable-Hbase replicator](https://github.com/GoogleCloudPlatform/DataflowTemplates/pull/780) to prevent replication loops. 

Optional parameters like the filtering keyword can be set with the following properties in `hbase-site.xml`.
```
<property>
    <name>google.bigtable.replication.hbase_qualifier</name>
    <value>some_magic_string</value>
</property>
<property>
    <name>google.bigtable.replication.cbt_qualifier</name>
    <value>some_other_magic_string</value>
</property>
```

## Deployment

Use the replication library version corresponding to your HBase version. For
HBase 1.x clusters, please use bigtable-hbase-1.x-replication.jar, for HBase 2.x
versions please use bigtable-hbase-2.x-replication.jar. Following are the steps
to configure HBase to Cloud Bigtable replication:

1. Install Cloud Bigtable replication library in HBase servers (both master and
   region servers).
    1. Download the library from maven on all the master and region server
       nodes.
    2. Copy the library to a folder in the HBase class path. For example copy
       the jar to /usr/lib/hbase/lib/
2. Add CBT configs to hbase-site.xml as discussed [above](#hbase-configuration).
   Specifically, `google.bigtable.project.id`
   , `google.bigtable.instance.id` and `google.bigtable.auth.json.keyfile` must
   be set.
4. Restart all the HBase master nodes by
   running `sudo systemctl restart hbase-master`, this allows the masters to
   load the replication jar and be aware of the classes in it. Users should
   follow their operational playbooks to perform a rolling restart of the HBase
   masters.
5. Restart all the region servers by
   running: `sudo systemctl restart hbase-regionserver`on each region server.
   Users should follow their operational playbooks to perform a rolling restart
   of the HBase cluster.
6. HBase's replication can be enabled at a cluster level/table level or column
   family level. `TABLE_CFS` is used to specify column families that should be
   replicated. Enable replication to CBT by running this command in hbase shell:
   `add_peer '<PEER_ID>', ENDPOINT_CLASSNAME => '
   com.google.cloud.bigtable.hbase.HbaseReplicationEndpoint, TABLE_CFS => { "
   table1" => [], "table2" => ["cf1"], "table3" => ["cf1", "cf2"] }`
7. All the replicated tables/column families must be present in the target Cloud
   Bigtable instance. When you enable HBase replication, changes from the
   beginning of the current WAL log will be replicated. Meaning, you will see
   changes from before the replication was enabled in Cloud Bigtable. This
   behavior is consistent with enabling replication with an HBase cluster.
8. Use your operational playbooks to monitor replication metrics. CBT HBase
   replication library will emit standard HBase replication peer metrics.
9. Users can also monitor replication status by running `status 'replication'`
   in HBase shell. The metrics for CBT replication will be under the “peer_id”
   used in the previous step.

## Incompatible Mutations

Certain HBase delete APIs
are [not supported on CBT](https://cloud.google.com/bigtable/docs/hbase-differences#mutations_and_deletions)
. If such mutations are issued on HBase, the CBT client in the replication
library will fail to propagate them and all replication to CBT endpoint will
stall. To avoid such stalling, we will log such mutations and skip them.
Following is a summary of unsupported operations and some supported operations
that can be modified during WAL write.

|Type of mutation |HBase WAL Write behavior |CBT replication library action|
|-----------------|-------------------------|------------------------------|
|DeleteLatestVersion|Resolves the latest version and writes a deletecell with timestamp|Supported, as its normal deletecell|
|DeleteFamilyAtVersion|Not modified|Logged and skipped|
|DeleteFamilyBeforeTimestamp|Not modified|Converts it to DeleteFamily if timestamp within a configurable threshold.|
|DeleteFamily|Converts to DeleteFamilyBeforeTimestamp with timestamp=now|See DeleteFamilyBeforeTimestamp|
|DeleteRow|Converts to DeleteFamilyBeforeTimestamp with timestamp=now for all families|See DeleteFamilyBeforeTimestamp|

The goal of this replication library is to allow migration from HBase to CBT.
Since CBT will not support these mutations after the users migrate to CBT, they
are recommended to come up with alternative ways to handle these incompatible
APIs and not issue them while replication is on.

Another special case is mutations with custom cell timestamps. HBase uses
a `long`
to store milliseconds while Cloud Bigtable uses `long` to store microseconds.
This [difference in granularity](https://cloud.google.com/bigtable/docs/hbase-differences#timestamps)
means, HBase can store 1000 times higher cell timestamps than Cloud Bigtable.
The impacted use case is the custom cell timestamp, where customers
use `Long.MAX_VALUE - now` as the cell timestamp. Such timestamps may get
truncated in Cloud Bigtable.

Users can inject custom implementation of IncompatibleMutationHandler. Please
refer
to [IncompatibleMutationAdapter](bigtable-hbase-replication-core/src/main/java/com/google/cloud/bigtable/hbase/replication/adapters/IncompatibleMutationAdapter.java)
javadocs for more details.

### Dry run mode

It may be hard to determine if an application issues incompatible mutations,
especially if the HBase cluster and application are owned by different teams.
The replication library provides a dry-run mode to detect incompatible
mutations. In dry run mode, replication library checks the mutations for
incompatibility and never sends them to Cloud Bigtable. All the incompatible
mutations are logged. If you are not sure about incompatible mutations, enable
replication in the dry run mode and observe the incompatible mutation metrics (
discussed below).

You should make sure that all the [prerequisites](#prerequisites) are fulfilled
before enabling the dry run mode. Dry run mode can be enabled by setting the
property `google.bigtable.replication.enable_dry_run` to true. It can be set
in `hbase-site.xml` but we recommend setting it during peer creation.
Enabling/disabling dry run mode during peer creation can avoid restarting the
HBase cluster to pickup changes to `hbase-site.xml` file. Enable dry run mode by
running the following command to add Cloud Bigtable replication peer (please
change the endpoint class
to `com.google.cloud.bigtable.hbase1_x.replication.HbaseToCloudBigtableReplicationEndpoint`
for HBase 1.x):

```
add_peer 'peer_id',
 ENDPOINT_CLASSNAME=>'com.google.cloud.bigtable.hbase2_x.replication.HbaseToCloudBigtableReplicationEndpoint',
  CONFIG=>{'google.bigtable.replication.enable_dry_run' => 'true' }
```

When you are ready to enable replication to Cloud Bigtable, delete this peer and
create a new peer in normal mode (**do not** try to update the "dry-run" peer):

```
remove_peer 'peer_id'
add_peer 'new_peer_id', ENDPOINT_CLASSNAME=>'com.google.cloud.bigtable.hbase2_x.replication.HbaseToCloudBigtableReplicationEndpoint'
```

## Error handling

HBase has push based replication. Each region server reads the WAL entries and
passes them to each replication endpoint. If the replication endpoint fails to
apply WAL logs, the WAL will accumulate on HBase regions servers.

If a Bigtable cluster is temporarily unavailable, the WAL logs will accumulate
on region servers. Once the cluster becomes available again, replication can
continue.

For any non-retryable error, like non-existent column-family, replication will
pause and WAL logs will build-up. Users should monitor & alert on replication
progress
via [HBase replication monitoring](https://hbase.apache.org/book.html#_monitoring_replication_status)
. The replication library can not skip a replication entry as a single WAL entry
represents an atomic transaction. Skipping a message will result in divergence
between source and target tables.

## Monitoring

The replication library will emit the metrics into HBase metric ecosystem. There
are 3 kinds of metrics that the replication library will publish:

1. HBase will
   track [replication metrics](https://hbase.apache.org/book.html#_replication_metrics)
   for the CBT peer on its own. These
   include [metrics](https://hbase.apache.org/book.html#_understanding_the_output)
   for replication sinks. For example, AgeOfLastShippedLog etc.
2. Cloud Bigtable client side metrics. These will include latencies and failures
   of various CBT APIs.
3. Custom metrics from the replication library. For example,
   NumberOfIncompatibleMutations. Please note that cusotm metrics support is
   available for HBase 1.4 or newer.

Please refer to javadocs for class HBaseToCloudBigtableReplicationMetrics for
list of available metrics.

## Troubleshooting

### Replication stalling

#### Symptom: High value of ageOfLastShippedOp.

If the remedial action is not taken it will result in disk full errors.

#### Causes and remediation

Following are the possible causes of replication stalling:

- **Incompatible mutations**: Incompatible mutations can stall replication as
  the CBT client will fail to ship them and return a permanent error. Default
  IncompatibleMutationHandler strategies shipped with CBTEndpoint log and drop
  the incompatible mutations to prevent replication from stalling. If users are
  providing custom IncompatibleMutationHandling strategies, they must make sure
  that all incompatible mutations are either adapted or dropped.
- **CBT resources do not exist**: If any of the CBT resources do not exist,
  replication will stall. Users can resume the replication by creating the
  appropriate resources. Most common case will be creation of a new replicated
  HBase table which is not present on CBT.
- **Unavailability of CBT cluster**: We recommend users to use a single cluster
  routing app profile to ship changes to CBT, as it guarantees ordering of
  mutations. But if the target cluster is down, replication will stall. For
  larger outages, users may need to route the traffic to other clusters or just
  wait for CBT to be available again.
- **Slow CBT cluster**: An under-provisioned CBT cluster can cause replication
  to slow down or even stall. Users should monitor the CBT cluster’s CPU and
  ideally keep it under 80% utilization. Cloud Bigtable now supports autoscaling
  which allows the cluster to scale up when CPU utilization is high.

### Inconsistencies between HBase and CBT

If there are widespread inconsistencies between HBase and CBT, users may need to
restart the migration from the beginning.

#### Causes and remediation

Following are potential causes and steps to avoid them

- **CBTEndpoint using multi cluster routing**: If HBase replication uses multi
  cluster routing, it may write to different clusters and CBT replication’s
  “last writer wins” conflict resolution may lead to a different order of
  mutation than HBase’s ordering.
- **Dropped incompatible mutations**: Users should stop using incompatible
  mutations before starting the replication to CBT, since they will not have
  access to these APIs after migration. If CBTEndpoint drops the incompatible
  mutations, 2 databases will diverge.
- **HBase replication gaps**: There
  are [cases](https://hbase.apache.org/book.html#_serial_replication) when HBase
  replication does not converge (it is rare). Such cases can lead to divergence.
  These are unavoidable and users should perform remediation at the migration
  verification step.

### Big Red Button

In extreme situations, Cloud Bigtable may pose a risk to HBase availability. For
example, replication is stalled and cluster is running out of HDFS storage. In
such cases, users should use the big red button and disable replication. Users
should use
`delete_peer 'peer_id'` command from HBase shell. This will delete the Cloud
Bigtable replication peer and allow the WAL logs to be garbage collected. 
