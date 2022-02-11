# HBase-Bigtable Replication

## Overview

This project replicates HBase tables to Cloud Bigtable tables.

## Requirements

- HBase cluster is configured with the setting `hbase.replication` to `true` in hbase-site.xml

## Properties
Have the below properties set in `hbase-site.xml` and add it to the HBase region server classpath.
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

## Deployment
1. Add bigtable-hbase-replication-X.Y.jar and hbase-site.xml with the required properties to all the HBase region servers classpath and restart them.