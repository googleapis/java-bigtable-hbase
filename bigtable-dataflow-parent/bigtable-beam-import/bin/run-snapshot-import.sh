#!/bin/bash

# ==============================================================================
# HBase Snapshot Import Helper Script
# ==============================================================================
# This script runs a range of Dataflow snapshot import jobs sequentially or in parallel.
# Can be run from any directory, e.g., 'bin/run-snapshot-import.sh'.
#
# For detailed usage and advanced options, see: SNAPSHOT_IMPORT_USAGE.md
# ==============================================================================

# ------------------------------------------------------------------------------
# Environment Variables
# ------------------------------------------------------------------------------
# Most users will need to set these variables before running the script.
# See SNAPSHOT_IMPORT_USAGE.md for details and expected values.

# --- Required / Common Configurations ---
# export PROJECT_ID="your-project-id"
# export INSTANCE_ID="your-instance-id"
# export BUCKET="your-gcs-bucket"
# export REGION="us-central1"
# 
# export TABLE_NAME="your-table-name"
# export SNAPSHOT_NAME="your-snapshot-name"
# export SNAPSHOT_SOURCE_DIR="gs://your-gcs-bucket/snapshots"
# export SERVICE_ACCOUNT="your-service-account"

# --- Sharding & Tuning ---
# A 'shard' is a partition of HBase regions. Sharding allows splitting the import into
# multiple independent Dataflow jobs running in parallel to speed up the migration.
# export NUM_SHARDS="20"
# export MAX_INFLIGHT_RPCS="100"
# export BULK_MUTATION_CLOSE_TIMEOUT_MINUTES="30"

# --- Network Configurations ---
# export NETWORK="your-network"
# export SUBNETWORK="your-subnetwork"

# ------------------------------------------------------------------------------
# Usage
# ------------------------------------------------------------------------------
# Usage: bin/run-snapshot-import.sh <start_shard> <end_shard>
#        (Both start and end shard indices are inclusive)
#   Or:  bin/run-snapshot-import.sh --all
#        (Runs all shards in parallel groups of 4 by default)
#
# Examples:
#   bin/run-snapshot-import.sh 0 3
#   bin/run-snapshot-import.sh --all

# Validate and parse arguments
if [ "$1" != "--all" ] && [ "$1" != "--restore-only" ]; then
    if [ -z "$1" ] || [ -z "$2" ]; then
        echo "Usage: $0 <start_shard> <end_shard> [additional_args...]"
        echo "        (Both start and end shard indices are inclusive)"
        echo "   Or: $0 --all [additional_args...]"
        echo "   Or: $0 --restore-only [additional_args...]"
        exit 1
    fi
    START_SHARD=$1
    END_SHARD=$2
    EXTRA_ARGS=("${@:3}")
else
    EXTRA_ARGS=("${@:2}")
fi

# Validate required environment variables
REQUIRED_VARS=(PROJECT_ID INSTANCE_ID BUCKET REGION TABLE_NAME SNAPSHOT_NAME SNAPSHOT_SOURCE_DIR)
for var in "${REQUIRED_VARS[@]}"; do
    if [ -z "${!var}" ]; then
        echo "❌ Error: Environment variable $var is not set."
        exit 1
    fi
done

if [ "$1" != "--restore-only" ] && [ -z "${NUM_SHARDS}" ]; then
    echo "❌ Error: Environment variable NUM_SHARDS is not set."
    exit 1
fi

# Set default values for optional tuning parameters
MAX_INFLIGHT_RPCS="${MAX_INFLIGHT_RPCS:-100}"
BULK_MUTATION_CLOSE_TIMEOUT_MINUTES="${BULK_MUTATION_CLOSE_TIMEOUT_MINUTES:-30}"

# Configurations
SCRIPT_DIR=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" &> /dev/null && pwd)

# Detect the newest shaded JAR dynamically in the target directory
JAR_PATH=""
for jar in "${SCRIPT_DIR}"/../target/bigtable-beam-import-*-shaded.jar; do
    if [ -f "$jar" ]; then
        if [ -z "${JAR_PATH}" ] || [ "$jar" -nt "${JAR_PATH}" ]; then
            JAR_PATH="$jar"
        fi
    fi
done

if [ -z "${JAR_PATH}" ]; then
    echo "❌ Error: Shaded JAR not found in ${SCRIPT_DIR}/../target/"
    echo "   Please compile and package the project first by running:"
    echo "   mvn clean package -DskipTests"
    exit 1
fi
RESTORE_DIR="gs://${BUCKET}/restore-${SNAPSHOT_NAME}"
# If running the launcher on a newer, unsupported JDK version (e.g. JDK 25+),
# you can set JVM_OPTS to "-Dnet.bytebuddy.experimental=true" to bypass
# ByteBuddy class generation crashes during pipeline construction.
JVM_OPTS="${JVM_OPTS:-}"

# Construct optional network arguments
NETWORK_ARGS=()
if [ -n "${NETWORK}" ]; then
    NETWORK_ARGS+=("--network=${NETWORK}")
fi
if [ -n "${SUBNETWORK}" ]; then
    NETWORK_ARGS+=("--subnetwork=${SUBNETWORK}")
fi

# Construct optional service account arguments
SERVICE_ACCOUNT_ARGS=()
if [ -n "${SERVICE_ACCOUNT}" ]; then
    SERVICE_ACCOUNT_ARGS+=("--serviceAccount=${SERVICE_ACCOUNT}")
fi


# --- RESTORE ONLY MODE ---
if [ "$1" == "--restore-only" ]; then
    echo "🚀 Performing snapshot restore (blocking)..."
    java ${JVM_OPTS} -jar "${JAR_PATH}" importsnapshot \
      --runner=DataflowRunner \
      --project="${PROJECT_ID}" \
      --bigtableInstanceId="${INSTANCE_ID}" \
      --bigtableTableId="${TABLE_NAME}" \
      --hbaseSnapshotSourceDir="${SNAPSHOT_SOURCE_DIR}" \
      --snapshots="${SNAPSHOT_NAME}:${TABLE_NAME}" \
      --stagingLocation="gs://${BUCKET}/dataflow/staging" \
      --tempLocation="gs://${BUCKET}/dataflow/temp" \
      --region="${REGION}" \
      --performOnlyRestoreStep=true \
      --restorePath="${RESTORE_DIR}" \
      --jobName="restore-job" \
      "${SERVICE_ACCOUNT_ARGS[@]}" \
      "${NETWORK_ARGS[@]}" \
      "${EXTRA_ARGS[@]}"
    echo "✅ Restore completed."
    echo "⚠️ IMPORTANT: Please manually cleanup the restore path once validation succeeds:"
    echo "   gsutil rm -r \"${RESTORE_DIR}\""
    exit 0
fi

# --- AUTO-PARALLEL MODE ---
if [ "$1" == "--all" ]; then
    echo "🚀 Starting fully automated snapshot import..."
    
    # Step 1: Perform ONLY the restore step
    echo "Step 1/2: Performing snapshot restore (blocking)..."
    java ${JVM_OPTS} -jar "${JAR_PATH}" importsnapshot \
      --runner=DataflowRunner \
      --project="${PROJECT_ID}" \
      --bigtableInstanceId="${INSTANCE_ID}" \
      --bigtableTableId="${TABLE_NAME}" \
      --hbaseSnapshotSourceDir="${SNAPSHOT_SOURCE_DIR}" \
      --snapshots="${SNAPSHOT_NAME}:${TABLE_NAME}" \
      --stagingLocation="gs://${BUCKET}/dataflow/staging" \
      --tempLocation="gs://${BUCKET}/dataflow/temp" \
      --region="${REGION}" \
      --performOnlyRestoreStep=true \
      --restorePath="${RESTORE_DIR}" \
      --jobName="restore-job" \
      "${SERVICE_ACCOUNT_ARGS[@]}" \
      "${NETWORK_ARGS[@]}" \
      "${EXTRA_ARGS[@]}"
      
    echo "Restore completed. Proceeding to data import."

    # Step 2: Launch 4 parallel runner streams
    echo "Step 2/2: Launching 4 parallel runner streams..."
    MAX_CONCURRENCY=4
    SHARDS_PER_RUNNER=$(( (NUM_SHARDS + MAX_CONCURRENCY - 1) / MAX_CONCURRENCY ))
    
    pids=()
    for (( runner=0; runner<MAX_CONCURRENCY; runner++ )); do
        start=$(( runner * SHARDS_PER_RUNNER ))
        end=$(( start + SHARDS_PER_RUNNER - 1 ))
        [ $start -ge $NUM_SHARDS ] && break
        [ $end -ge $NUM_SHARDS ] && end=$((NUM_SHARDS - 1))
        
        echo "Launching runner $runner: shards $start to $end in background"
        # Call ourselves with the range and propagate any extra arguments!
        "${BASH_SOURCE[0]}" $start $end "${EXTRA_ARGS[@]}" &
        pids+=($!)
    done
    
    echo "All groups launched. Waiting for all background jobs to finish..."
    failed=0
    for pid in "${pids[@]}"; do
        if ! wait "$pid"; then
            failed=1
        fi
    done

    if [ $failed -ne 0 ]; then
        echo "❌ Error: One or more background import jobs failed."
        exit 1
    fi
    
    echo "🎉 All import jobs completed!"
    echo "⚠️ IMPORTANT: Please manually cleanup the restore path once validation succeeds:"
    echo "   gsutil rm -r \"${RESTORE_DIR}\""
    exit 0
fi
# ----------------------------------------

# Standard Range Mode
for (( i=START_SHARD; i<=END_SHARD; i++ )); do
  echo "Submitting Dataflow job for shardIndex: $i"

  # As per the sharding contract, ALL parallel sharded jobs MUST skip the restore step
  # to prevent concurrent shards from deleting the restore path.
  # The --all mode runs performOnlyRestoreStep=true automatically in Step 1.
  SKIP_RESTORE="true"
  
  JOB="job-${i}"
  java ${JVM_OPTS} -jar "${JAR_PATH}" importsnapshot \
    --runner=DataflowRunner \
    --project="${PROJECT_ID}" \
    --bigtableInstanceId="${INSTANCE_ID}" \
    --bigtableTableId="${TABLE_NAME}" \
    --hbaseSnapshotSourceDir="${SNAPSHOT_SOURCE_DIR}" \
    --snapshots="${SNAPSHOT_NAME}:${TABLE_NAME}" \
    --stagingLocation="gs://${BUCKET}/dataflow/staging" \
    --tempLocation="gs://${BUCKET}/dataflow/temp" \
    --workerMachineType=n1-highmem-4 \
    --diskSizeGb=500 \
    --maxNumWorkers=10 \
    --region="${REGION}" \
    "${SERVICE_ACCOUNT_ARGS[@]}" \
    --usePublicIps=false \
    --enableSnappy=true \
    --skipRestoreStep="${SKIP_RESTORE}" \
    --deleteRestoredSnapshots=false \
    --restorePath="${RESTORE_DIR}" \
    --numShards="${NUM_SHARDS}" \
    --shardIndex="${i}" \
    --jobName="${JOB}" \
    "${NETWORK_ARGS[@]}" \
    --maxInflightRpcs="${MAX_INFLIGHT_RPCS}" \
    --bulkMutationCloseTimeoutMinutes="${BULK_MUTATION_CLOSE_TIMEOUT_MINUTES}" \
    "${EXTRA_ARGS[@]}"

  # Sequential within this script instance
done
