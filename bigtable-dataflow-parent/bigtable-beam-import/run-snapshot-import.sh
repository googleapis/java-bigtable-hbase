#!/bin/bash

# This script runs a range of Dataflow snapshot import jobs sequentially.
# It should be executed from the 'bigtable-dataflow-parent/bigtable-beam-import' directory.
#
# Usage: ./run-snapshot-import.sh <start_shard> <end_shard>
#   Or:  ./run-snapshot-import.sh --all
# Example: ./run-snapshot-import.sh 0 3
# Example: ./run-snapshot-import.sh --all
#
# You can override default configurations by setting environment variables in your terminal.
# Example: TABLE_NAME="my-table" SNAPSHOT_NAME="my-snap" ./run-snapshot-import.sh 0 3
#
# NOTE: If you are running on a newer JDK (like Java 21 or 26) and hit ByteBuddy errors,
# you can add '-Dnet.bytebuddy.experimental=true' to the java command lines below.
#
# --- Manual Parallel Execution ---
# To run shards in parallel groups of 4 (assuming 20 shards total), you can run 5 instances of this script.
#
# IMPORTANT: Shard 0 performs the restore step. You MUST run the first group (including shard 0)
# first and let it complete the restore step before launching other groups in parallel, 
# otherwise they will fail because the restored files won't exist yet!
#
# Example for manual parallel execution:
#   ./run-snapshot-import.sh 0 3 &  # Run this first!
#   # Wait for shard 0 to finish restore, then run the rest:
#   ./run-snapshot-import.sh 4 7 &
#   ./run-snapshot-import.sh 8 11 &
#   ./run-snapshot-import.sh 12 15 &
#   ./run-snapshot-import.sh 16 19 &
#
# --- Automated Parallel Execution ---
# Alternatively, use the --all flag to automatically handle the restore step and launch all groups:
#   ./run-snapshot-import.sh --all

if [ "$#" -ne 2 ] && [ "$1" != "--all" ]; then
    echo "Usage: $0 <start_shard> <end_shard>"
    echo "   Or: $0 --all"
    exit 1
fi

START_SHARD=$1
END_SHARD=$2

# Configurations (Uses environment variables if set, otherwise defaults)
export PROJECT_ID="${PROJECT_ID:-google.com:cloud-bigtable-dev}"
export INSTANCE_ID="${INSTANCE_ID:-tianlei-test-inst}"
export BUCKET="${BUCKET:-tianlei-beam-test-bucket}"
export REGION="${REGION:-us-central1}"

export TABLE_NAME="${TABLE_NAME:-validation_test}"
export SNAPSHOT_NAME="${SNAPSHOT_NAME:-validation_test_20200929}"
export SERVICE_ACCOUNT="${SERVICE_ACCOUNT:-295490517436-compute@developer.gserviceaccount.com}"

export NUM_SHARDS="${NUM_SHARDS:-20}"

export NETWORK="${NETWORK:-tianlei-network}"
export SUBNETWORK="${SUBNETWORK:-regions/us-central1/subnetworks/tianlei-network}"

JAR_PATH="target/bigtable-beam-import-2.17.0-shaded.jar"

# --- AUTO-PARALLEL MODE ---
if [ "$1" == "--all" ]; then
    echo "🚀 Starting fully automated snapshot import..."
    
    # Step 1: Perform ONLY the restore step
    echo "Step 1/2: Performing snapshot restore (blocking)..."
    java -jar ${JAR_PATH} importsnapshot \
      --runner=DataflowRunner \
      --project=${PROJECT_ID} \
      --bigtableInstanceId=${INSTANCE_ID} \
      --bigtableTableId=${TABLE_NAME} \
      --importConfigFilePath=import-config-test.json \
      --stagingLocation=gs://${BUCKET}/dataflow/staging \
      --tempLocation=gs://${BUCKET}/dataflow/temp \
      --region=${REGION} \
      --performOnlyRestoreStep=true \
      --jobName="restore-job" \
      --network=${NETWORK} \
      --subnetwork=${SUBNETWORK}
      
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
    exit 0
fi
# ----------------------------------------

# Standard Range Mode
for i in $(seq $START_SHARD $END_SHARD); do
  echo "Submitting Dataflow job for shardIndex: $i"

  # We skip restore for all shards if running via --all because Step 1 handled it.
  # If running manually via ranges, shard 0 will perform restore.
  SKIP_RESTORE="true"
  if [ $i -eq 0 ]; then
    SKIP_RESTORE="false"
  fi
  
  JOB="job-${i}"
  java -jar ${JAR_PATH} importsnapshot \
    --runner=DataflowRunner \
    --project=${PROJECT_ID} \
    --bigtableInstanceId=${INSTANCE_ID} \
    --bigtableTableId=${TABLE_NAME} \
    --importConfigFilePath=import-config-test.json \
    --stagingLocation=gs://${BUCKET}/dataflow/staging \
    --tempLocation=gs://${BUCKET}/dataflow/temp \
    --workerMachineType=n1-highmem-4 \
    --diskSizeGb=500 \
    --maxNumWorkers=10 \
    --region=${REGION} \
    --serviceAccount=${SERVICE_ACCOUNT} \
    --usePublicIps=false \
    --enableSnappy=true \
    --skipRestoreStep=${SKIP_RESTORE} \
    --numShards=${NUM_SHARDS} \
    --shardIndex=$i \
    --jobName="${JOB}" \
    --network=${NETWORK} \
    --subnetwork=${SUBNETWORK}

  # Sequential within this script instance
done
