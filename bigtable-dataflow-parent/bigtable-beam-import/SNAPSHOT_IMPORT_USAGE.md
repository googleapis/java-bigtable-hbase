# HBase Snapshot Import Helper Script Usage

This document describes the environment variables used by the `bin/run-snapshot-import.sh` script to automate HBase snapshot imports into Cloud Bigtable using Dataflow.

## Environment Variables

The script relies on the following environment variables. You should set them before executing the script.

| Variable | Status | Description | Default / Example |
| :--- | :--- | :--- | :--- |
| `PROJECT_ID` | **Required** | The Google Cloud Project ID where the Bigtable instance and Dataflow jobs reside. | `your-project-id` |
| `INSTANCE_ID` | **Required** | The Bigtable Instance ID to import data into. | `your-instance-id` |
| `BUCKET` | **Required** | The GCS bucket name. The script automatically strips any leading gs:// or trailing slashes for robust path construction. | `your-gcs-bucket` |
| `REGION` | **Required** | The GCP region to run the Dataflow jobs in. | `us-central1` |
| `TABLE_NAME` | **Required** | The target Bigtable table name. | `your-table-name` |
| `SNAPSHOT_NAME` | **Required** | The name of the HBase snapshot to import. | `your-snapshot-name` |
| `SNAPSHOT_SOURCE_DIR` | **Required** | The GCS path where the HBase snapshot export is located. | `gs://your-gcs-bucket/snapshots` |
| `NUM_SHARDS` | **Required** (except `--restore-only`) | The number of shards to split the import into for parallel processing. | `20` |
| `SERVICE_ACCOUNT` | *Optional* | The service account email to run the Dataflow jobs. | `your-service-account@developer.gserviceaccount.com` |
| `MAX_INFLIGHT_RPCS` | *Optional* | Maximum number of inflight RPCs for Bigtable client. | `100` (Default) |
| `BULK_MUTATION_CLOSE_TIMEOUT_MINUTES` | *Optional* | Timeout in minutes for closing bulk mutations. | `30` (Default) |
| `NETWORK` | *Optional* | VPC Network name for Dataflow workers. | `your-network` |
| `SUBNETWORK` | *Optional* | VPC Subnetwork name for Dataflow workers. | `regions/us-central1/subnetworks/your-subnetwork` |
| `JOB_NAME_PREFIX` | *Optional* | Safe, unique prefix for all Dataflow jobs to prevent name collisions. | `import-<sanitized_table_name>` (Default) |
| `WORKER_MACHINE_TYPE` | *Optional* | Compute Engine machine type for Dataflow workers. | `n1-highmem-4` (Default) |
| `DISK_SIZE_GB` | *Optional* | Worker disk size in Gigabytes. | `500` (Default) |
| `MAX_NUM_WORKERS` | *Optional* | Maximum number of active Dataflow workers per job. | `10` (Default) |
| `USE_PUBLIC_IPS` | *Optional* | Whether Dataflow workers should have public IPs. | `false` (Default) |
| `ENABLE_SNAPPY` | *Optional* | Whether to enable Snappy compression for transit files. | `true` (Default) |

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
> Shard indices must be non-negative integers strictly less than `NUM_SHARDS` (i.e. in the range `[0, NUM_SHARDS)`). The script validates this range and will fail fast if violated.

```bash
bin/run-snapshot-import.sh <start_shard> <end_shard>
```
Example: `bin/run-snapshot-import.sh 0 5`

### Run all shards (Auto-parallel mode)
```bash
bin/run-snapshot-import.sh --all
```
This mode will first run the blocking restore step, and then partition the total shards evenly and launch exactly 4 parallel runner streams in the background to process them. This ensures that no more than 4 Dataflow jobs are ever running concurrently, protecting your Bigtable instance from write-throttling and staying safely within GCP resource quotas.

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
