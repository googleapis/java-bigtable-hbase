# HBase Snapshot Import Helper Script Usage

This document describes the environment variables used by the `bin/run-snapshot-import.sh` script to automate HBase snapshot imports into Cloud Bigtable using Dataflow.

## Environment Variables

The script relies on the following environment variables. You should set them before executing the script.

| Variable | Description | Example / Suggested Value |
| :--- | :--- | :--- |
| `PROJECT_ID` | The Google Cloud Project ID where the Bigtable instance and Dataflow jobs reside. | `your-project-id` |
| `INSTANCE_ID` | The Bigtable Instance ID to import data into. | `your-instance-id` |
| `BUCKET` | The GCS bucket name used for Dataflow staging, temp files, and default snapshot source path. | `your-gcs-bucket` |
| `REGION` | The GCP region to run the Dataflow jobs in. | `us-central1` |
| `TABLE_NAME` | The target Bigtable table name. | `your-table-name` |
| `SNAPSHOT_NAME` | The name of the HBase snapshot to import. | `your-snapshot-name` |
| `SNAPSHOT_SOURCE_DIR` | The GCS path where the HBase snapshot export is located. | `gs://your-gcs-bucket/snapshots` |
| `SERVICE_ACCOUNT` | The service account email to run the Dataflow jobs. | `your-service-account@developer.gserviceaccount.com` |
| `NUM_SHARDS` | The number of shards to split the import into for parallel processing. | `20` |
| `MAX_INFLIGHT_RPCS` | Maximum number of inflight RPCs for Bigtable client. | `100` |
| `BULK_MUTATION_CLOSE_TIMEOUT_MINUTES` | Timeout in minutes for closing bulk mutations. | `30` |
| `NETWORK` | VPC Network name for Dataflow workers. | `your-network` |
| `SUBNETWORK` | VPC Subnetwork name for Dataflow workers. | `regions/us-central1/subnetworks/your-subnetwork` |

## Understanding Sharding

An HBase snapshot consists of multiple physical **regions** (ordered chunks of table keys). To migrate extremely large tables efficiently, you can partition (shard) the regions across multiple independent Dataflow jobs.

*   **How Sharding Works:** The import tool distributes regions uniformly across a total number of shards (`NUM_SHARDS`) by hashing each region's encoded name and taking it modulo `NUM_SHARDS`.
*   **Targeted Shards:** A Dataflow job configured with a specific `shardIndex` only reads and imports the HBase regions assigned to that particular shard.
*   **Why use Sharding:** Instead of running a single massive Dataflow job that might hit quota limits or VM memory constraints, you can split the import into multiple smaller, parallelizable jobs.

## Usage

### Run a specific shard range

Runs a range of shard indices sequentially within the script execution.

> [!NOTE]
> Both `<start_shard>` and `<end_shard>` are **inclusive**. E.g., running `bin/run-snapshot-import.sh 0 5` will submit and execute Dataflow jobs for shards `0, 1, 2, 3, 4, and 5` sequentially.

```bash
bin/run-snapshot-import.sh <start_shard> <end_shard>
```
Example: `bin/run-snapshot-import.sh 0 5`

### Run all shards (Auto-parallel mode)
```bash
bin/run-snapshot-import.sh --all
```
This mode will first run the restore step, and then launch background processes for all shards in parallel groups of 4 by default.

## Advanced Usage

### Manual Parallel Execution

To run shards in parallel groups (e.g., assuming 20 shards total), you can run multiple instances of this script.

> [!IMPORTANT]
> Because concurrent shards cannot delete or overwrite the restored snapshot directory simultaneously, **no shard** performs the restore step during a sharded run. You MUST run the restore step explicitly first!

Example for manual parallel execution:
```bash
# 1. Run the blocking restore step first!
bin/run-snapshot-import.sh --restore-only

# 2. Once the restore is complete, launch shards in parallel:
bin/run-snapshot-import.sh 0 3 &
bin/run-snapshot-import.sh 4 7 &
bin/run-snapshot-import.sh 8 11 &
bin/run-snapshot-import.sh 12 15 &
bin/run-snapshot-import.sh 16 19 &
```

## Troubleshooting

### JDK Compatibility

If you are running on a newer, unsupported JDK version (e.g. JDK 25+) and hit ByteBuddy errors, you can simply set the `JVM_OPTS` variable to `"-Dnet.bytebuddy.experimental=true"` in the Configurations section of the `bin/run-snapshot-import.sh` script.
