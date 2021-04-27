#HBase Tools

This folder contains tools to help HBase users with migrations to Cloud Bigtable.
Pipelines to import and export data are under [bigtable-beam-import](bigtable-dataflow-parent/bigtable-beam-import/README.md).

## Setup 

To use the tools in this folder, you can download them from the maven repository, or
you can build them using Maven.

[//]: # ({x-version-update-start:bigtable-dataflow-parent:released})
### Download the jars

todo: get correct link, also should I provide a wget command?

Download [the Bigtable tools jars](http://search.maven.org/remotecontent?filepath=com/google/cloud/bigtable/bigtable-beam-import/1.14.1/bigtable-beam-import-1.14.1-shaded.jar), which is an aggregation of all required jars.

### Build the jars
Go to the top level directory and build the repo then return to this sub directory.

```
cd ../../
mvn clean install -DskipTests=true
cd bigtable-hbase-1.x-parent/bigtable-hbase-tools
```

## Schema Translation tool 
This utility tool will create tables in Cloud Bigtable based on the tables in an HBase cluster.
You specifiy a name regex and it will copy column families, garbage collection rules,
and table splits.
 
todo: confirm what it copies?   

todo: what is the regex format?

Define the environment variables to easily run the command.
```shell
PROJECT_ID=your-project-id
INSTANCE_ID=your-instance-id
TABLE_NAME_REGEX=your-table-name

ZOOKEEPER_QUORUM=localhost
ZOOKEEPER_PORT=2181
```

Execute the following command to copy the schema from HBase to Cloud Bigtable.

```
java -jar bigtable-hbase-tools-1.14.1-SNAPSHOT-jar-with-dependencies.jar \
  com.google.cloud.bigtable.hbase.tools.HBaseSchemaTranslator \
 -Dgoogle.bigtable.project.id=$PROJECT_ID \
 -Dgoogle.bigtable.instance.id=$INSTANCE_ID \
 -Dgoogle.bigtable.table.filter=$TABLE_NAME_REGEX \
 -Dhbase.zookeeper.quorum=$ZOOKEEPER_QUORUM \
 -Dhbase.zookeeper.property.clientPort=$ZOOKEEPER_PORT
```

### Alternative: Exporting Schema

If your HBase master is in a private VPC or can't connect to internet, you can
export the HBase schema to a file and use that to create tables in Cloud Bigtable.


#### Export schema

On a host that can connect to HBase, define an additional environment variable for the
export location for your schema file.

```
$HBASE_EXPORT_PATH=/path/to/hbase-schema
```

Run the export tool from the host.

```
java -jar bigtable-hbase-tools-1.14.1-SNAPSHOT-jar-with-dependencies.jar \
  com.google.cloud.bigtable.hbase.tools.HBaseSchemaTranslator \
 -Dgoogle.bigtable.table.filter=$TABLE_NAME_REGEX \
 -Dgoogle.bigtable.output.filepath=$SCHEMA_FILE_PATH
 -Dhbase.zookeeper.quorum=$ZOOKEEPER_QUORUM \
 -Dhbase.zookeeper.property.clientPort=$ZOOKEEPER_PORT \
```


#### Import schema

Copy the schema file to a host which can connect to Google Cloud. 

todo: how to do if it can't connect to the internet?

```
$SCHEMA_FILE_PATH=/path/to/hbase-schema
cp $HBASE_EXPORT_PATH $SCHEMA_FILE_PATH
```

Create tables in Cloud Bigtable using the schema file:

```
java -jar bigtable-hbase-tools-1.14.1-SNAPSHOT-jar-with-dependencies.jar \
 com.google.cloud.bigtable.hbase.tools.HBaseSchemaTranslator \
 -Dgoogle.bigtable.project.id=$PROJECT_ID \
 -Dgoogle.bigtable.instance.id=$INSTANCE_ID \
 -Dgoogle.bigtable.input.filepath=$SCHEMA_FILE_PATH \
```
