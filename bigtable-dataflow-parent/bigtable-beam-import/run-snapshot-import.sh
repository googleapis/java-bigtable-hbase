#!/bin/bash

# This script runs the Dataflow snapshot import job.
# It should be executed from the 'bigtable-dataflow-parent/bigtable-beam-import' directory.

export PROJECT_ID=db-blackbelt-cndb
export INSTANCE_ID=bench-workload
export TABLE_NAME=validation_test
export SNAPSHOT_NAME=validation_test_20200929
export SERVICE_ACCOUNT=295490517436-compute@developer.gserviceaccount.com

export BUCKET=jh-data-sandbox-backups
export REGION=us-west1

# Using version 2.17.0 as per the current project version
JAR_PATH="target/bigtable-beam-import-2.17.0-shaded.jar"

echo "Submitting Dataflow job for shardIndex: 0. The initial job will restore the snapshot to the first iteration and will skip this step for subsequent jobs"
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
  --skipRestoreStep=false \
  --numShards=20 \
  --shardIndex=0

# Loop from 1 to 19
for i in {1..19}; do
  echo "Submitting Dataflow job for shardIndex: $i"

  JOB="job ${i}"
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
    --skipRestoreStep=true \
    --numShards=20 \
    --shardIndex=$i \
    --jobName="${JOB}" &

  # Optional: Sleep briefly between submissions to avoid API rate limits
  sleep 5
done
