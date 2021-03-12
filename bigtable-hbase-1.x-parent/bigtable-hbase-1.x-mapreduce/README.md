# Map/Reduce jobs to import from/export to Bigtable

This module provides a work alike to some of the jobs implemented in hbase-server.
Specifically this currently the ability to export and import SequenceFile from/to
Cloud Bigtable using a Map Reduce cluster (ie. dataproc).

## Expected Usage 

### Hadoop
1. Download and extract hbase 1.4.13 from https://hbase.apache.org/downloads.html
2. Configure you hbase-site.xml file to your hbase cluster
3. Download the latest shaded version of this module `bigtable-hbase-1.x-mapreduce-shaded-XXX.jar`
4. Download your service account credentials json file from google cloud console
5. Start the job:
    To export:
        ```
        HBASE_CLASSPATH=path/to/bigtable-hbase-1.x-mapreduce-shaded-XXX.jar \
        GOOGLE_APPLICATION_CREDENTIALS=path/to/service-account.json \
        hbase com.google.cloud.bigtable.mapreduce.Driver \
            export-table \
            -Dgoogle.bigtable.project.id=<project-id> \
            -Dgoogle.bigtable.instance.id=<instance-id> \
            <table-id> \
            <outputdir>
        ```
    
    To import:
        ```
        HBASE_CLASSPATH=path/to/bigtable-hbase-1.x-mapreduce-shaded-XXX.jar \
        GOOGLE_APPLICATION_CREDENTIALS=path/to/service-account.json \
        hbase com.google.cloud.bigtable.mapreduce.Driver \
                    import-table \
                    -Dgoogle.bigtable.project.id=<project-id> \
                    -Dgoogle.bigtable.instance.id=<instane-id> \
                    <table-id> \
                    <inputdir>
        ```

### Dataproc
TODO