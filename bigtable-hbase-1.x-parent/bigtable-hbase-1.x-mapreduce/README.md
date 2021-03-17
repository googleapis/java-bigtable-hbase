# Map/Reduce jobs to import from/export to Bigtable

This module provides a work alike to some of the jobs implemented in hbase-server.
Specifically this currently has the ability to export and import SequenceFile
from/to     Cloud Bigtable using a Map Reduce cluster (ie. dataproc).

## Expected Usage 

[//]: # ({x-version-update-start:bigtable-hbase-1x-parent:released})
### On-prem Hadoop

1. Download or build bigtable-hbase-1.x-mapreduce-1.14.0-shaded-byo-hadoop.jar
2. Download service account credentials json from Google Cloud Console.
3. Submit the job using your edge node's hadoop installation. 
   ```bash
   # Export
   GOOGLE_APPLICATION_CREDENTIALS=path/to/service-account.json \
   hadoop jar bigtable-hbase-1.x-mapreduce-1.14.0-shaded-byo-hadoop.jar \
       export-table \
       -Dgoogle.bigtable.project.id=<project-id> \
       -Dgoogle.bigtable.instance.id=<instance-id> \
       <table-id> \
       <outputdir>
   
   # Import
      GOOGLE_APPLICATION_CREDENTIALS=path/to/service-account.json \
      hadoop jar bigtable-hbase-1.x-mapreduce-1.14.0-shaded-byo-hadoop.jar \
          import-table \
          -Dgoogle.bigtable.project.id=<project-id> \
          -Dgoogle.bigtable.instance.id=<instance-id> \
          <table-id> \
          <inputdir>
   ```


### Dataproc

1. Download or build bigtable-hbase-1.x-mapreduce-1.14.0-shaded-byo-hadoop.jar.
2. Download the gcloud sdk.
3. Configure [Bigtable IAM roles](https://cloud.google.com/bigtable/docs/access-control#roles) 
    for the [Dataproc Service Account](https://cloud.google.com/dataproc/docs/concepts/configuring-clusters/service-accounts#what_are_service_accounts) 
    when running on Dataproc. Alternatively, download service account credentials json from Google Cloud Console.
4. Submit the job. 
   ```bash
    # Export
    gcloud dataproc jobs submit hadoop \
        --cluster <dataproc-cluster> \
        --region <dataproc-region> \
        --jar bigtable-hbase-1.x-mapreduce-1.14.0-shaded-byo-hadoop.jar \
        -- \
        export-table \
        -Dgoogle.bigtable.project.id=<project-id> \
        -Dgoogle.bigtable.instance.id=<instance-id> \
        <table-id> \
        <outputdir>
   
    # Import
    gcloud dataproc jobs submit hadoop \
        --cluster <dataproc-cluster> \
        --region <dataproc-region> \
        --jar bigtable-hbase-1.x-mapreduce-1.14.0-shaded-byo-hadoop.jar \
        -- \
        import-table \
        -Dgoogle.bigtable.project.id=<project-id> \
        -Dgoogle.bigtable.instance.id=<instance-id> \
        <table-id> \
        <inputdir>
   ```

[//]: # ({x-version-update-end})
