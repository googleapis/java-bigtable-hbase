#!/bin/bash

# ==============================================================================
# HBase Snapshot Import Helper Script
# ==============================================================================
# This script runs a range of Dataflow snapshot import jobs sequentially or in parallel.
# Must be executed from the 'bigtable-dataflow-parent/bigtable-beam-import' directory.
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
# export NUM_SHARDS="20"
# export MAX_INFLIGHT_RPCS="100"
# export BULK_MUTATION_CLOSE_TIMEOUT_MINUTES="30"

# --- Network Configurations ---
# export NETWORK="your-network"
# export SUBNETWORK="your-subnetwork"

# ------------------------------------------------------------------------------
# Usage
# ------------------------------------------------------------------------------
# Usage: ./run-snapshot-import.sh <start_shard> <end_shard>
#   Or:  ./run-snapshot-import.sh --all
#        (Runs all shards in parallel groups of 4 by default)
#
# Examples:
#   ./run-snapshot-import.sh 0 3
#   ./run-snapshot-import.sh --all

if [ "$#" -ne 2 ] && [ "$1" != "--all" ] && [ "$1" != "--restore-only" ]; then
    echo "Usage: $0 <start_shard> <end_shard>"
    echo "   Or: $0 --all"
    echo "   Or: $0 --restore-only"
    exit 1
fi

START_SHARD=$1
END_SHARD=$2

# Configurations
JAR_PATH="target/bigtable-beam-import-2.18.2-SNAPSHOT-shaded.jar"
RESTORE_DIR="gs://${BUCKET}/restore-${SNAPSHOT_NAME}"
# If running the launcher on a newer, unsupported JDK version (e.g. JDK 25+),
# you can set JVM_OPTS to "-Dnet.bytebuddy.experimental=true" to bypass
# ByteBuddy class generation crashes during pipeline construction.
JVM_OPTS=""

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
    java ${JVM_OPTS} -jar ${JAR_PATH} importsnapshot \
      --runner=DataflowRunner \
      --project=${PROJECT_ID} \
      --bigtableInstanceId=${INSTANCE_ID} \
      --bigtableTableId=${TABLE_NAME} \
      --hbaseSnapshotSourceDir=${SNAPSHOT_SOURCE_DIR} \
      --snapshots=${SNAPSHOT_NAME}:${TABLE_NAME} \
      --stagingLocation=gs://${BUCKET}/dataflow/staging \
      --tempLocation=gs://${BUCKET}/dataflow/temp \
      --region=${REGION} \
      --performOnlyRestoreStep=true \
      --restorePath=${RESTORE_DIR} \
      --jobName="restore-job" \
      "${NETWORK_ARGS[@]}"
    echo "✅ Restore completed."
    echo "⚠️ IMPORTANT: Please manually cleanup the restore path once validation succeeds:"
    echo "   gsutil rm -r ${RESTORE_DIR}"
    exit 0
fi

# --- AUTO-PARALLEL MODE ---
if [ "$1" == "--all" ]; then
    echo "🚀 Starting fully automated snapshot import..."
    
    # Step 1: Perform ONLY the restore step
    echo "Step 1/2: Performing snapshot restore (blocking)..."
    java ${JVM_OPTS} -jar ${JAR_PATH} importsnapshot \
      --runner=DataflowRunner \
      --project=${PROJECT_ID} \
      --bigtableInstanceId=${INSTANCE_ID} \
      --bigtableTableId=${TABLE_NAME} \
      --hbaseSnapshotSourceDir=${SNAPSHOT_SOURCE_DIR} \
      --snapshots=${SNAPSHOT_NAME}:${TABLE_NAME} \
      --stagingLocation=gs://${BUCKET}/dataflow/staging \
      --tempLocation=gs://${BUCKET}/dataflow/temp \
      --region=${REGION} \
      --performOnlyRestoreStep=true \
      --restorePath=${RESTORE_DIR} \
      --jobName="restore-job" \
      "${NETWORK_ARGS[@]}"
      
    echo "Restore completed. Proceeding to data import."

    # Step 2: Launch parallel groups of 4
    echo "Step 2/2: Launching parallel groups of 4 shards..."
    SHARDS_PER_GROUP=4
    
    for (( start=0; start<$NUM_SHARDS; start+=$SHARDS_PER_GROUP )); do
        end=$((start + SHARDS_PER_GROUP - 1))
        [ $end -ge $NUM_SHARDS ] && end=$((NUM_SHARDS - 1))
        
        echo "Launching group: shards $start to $end in background"
        # Call ourselves with the range!
        $0 $start $end &
    done
    
    echo "All groups launched. Waiting for all background jobs to finish..."
    wait
    echo "🎉 All import jobs completed!"
    echo "⚠️ IMPORTANT: Please manually cleanup the restore path once validation succeeds:"
    echo "   gsutil rm -r ${RESTORE_DIR}"
    exit 0
fi
# ----------------------------------------

# Standard Range Mode
for i in $(seq $START_SHARD $END_SHARD); do
  echo "Submitting Dataflow job for shardIndex: $i"

  # As per the sharding contract, ALL parallel sharded jobs MUST skip the restore step
  # to prevent concurrent shards from deleting the restore path.
  # The --all mode runs performOnlyRestoreStep=true automatically in Step 1.
  SKIP_RESTORE="true"
  
  JOB="job-${i}"
  java ${JVM_OPTS} -jar ${JAR_PATH} importsnapshot \
    --runner=DataflowRunner \
    --project=${PROJECT_ID} \
    --bigtableInstanceId=${INSTANCE_ID} \
    --bigtableTableId=${TABLE_NAME} \
    --hbaseSnapshotSourceDir=${SNAPSHOT_SOURCE_DIR} \
    --snapshots=${SNAPSHOT_NAME}:${TABLE_NAME} \
    --stagingLocation=gs://${BUCKET}/dataflow/staging \
    --tempLocation=gs://${BUCKET}/dataflow/temp \
    --workerMachineType=n1-highmem-4 \
    --diskSizeGb=500 \
    --maxNumWorkers=10 \
    --region=${REGION} \
    "${SERVICE_ACCOUNT_ARGS[@]}" \
    --usePublicIps=false \
    --enableSnappy=true \
    --skipRestoreStep=${SKIP_RESTORE} \
    --deleteRestoredSnapshots=false \
    --restorePath=${RESTORE_DIR} \
    --numShards=${NUM_SHARDS} \
    --shardIndex=$i \
    --jobName="${JOB}" \
    "${NETWORK_ARGS[@]}" \
    --maxInflightRpcs=${MAX_INFLIGHT_RPCS} \
    --bulkMutationCloseTimeoutMinutes=${BULK_MUTATION_CLOSE_TIMEOUT_MINUTES}

  # Sequential within this script instance
done
