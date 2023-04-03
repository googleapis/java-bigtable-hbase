# [Google Cloud Bigtable HBase adapter](https://cloud.google.com/bigtable/docs/bigtable-and-hbase)

[![Maven][maven-hbase-shield]][maven-hbase-client-maven-search]
[![Stack Overflow][stackoverflow-shield]][stackoverflow-link]

Libraries and tools for interoperability between Apache HBase and Google Cloud Bigtable:

* client adapters: allow applications written for HBase to run against Cloud Bigtable with minimal changes
* data migration pipelines: allow customers to use either Map/Reduce or Dataflow to migrate data from HBase to/from Cloud Bigtable
* dual write libraries: allow customers to replicate data from HBase to Cloud Bigtable

This project aims to support HBase version 1.0 - 2.3.6. Due to binary incompatibilities between major HBase versions,
support for each major version is provided by a different artifact (ie HBase 1.0 - 1.7.2 is supported by bigtable-hbase-1.x).
To avoid classpath conflicts, each artifact has a -shaded variant that bundles and relocates all  the dependencies in an uber jar.
All HBase and Hadoop dependencies must be provided by the end user's application.

## Summary of tools

* bigtable-hbase-1.x
  * Bigtable adapter for HBase 1.0-1.7.3
  * A mapreduce jobs for importing/exporting Sequence and HBase Snapshot files to/from Cloud Bigtable
  * A tool to copy HBase schemas to be Cloud Bigtable
* bigtable-hbase-2.x
   * Bigtable adapter for HBase 2.0-2.3.6
* hbase-migration-tools
  * bigtable-hbase-replication - library to enable replication from HBase Region servers to Cloud Bigtable
  * mirroring-client - a dual write client that writes to both HBase and Bigtable


## Project setup, installation, and configuration

### Prerequisites

* [Create a Cloud Bigtable instance](https://cloud.google.com/bigtable/docs/creating-instance)
* **Recommended**: [Install the Google Cloud SDK for Cloud Bigtable](https://cloud.google.com/bigtable/docs/installing-cloud-sdk)

### Installation

There are 2 ways to install the bigtable-hbase adapter:

#### Option 1:
1. Declare a dependency on bigtable-hbase:
   For applications using hbase-client or applications that want avoid dependency conflict please use the -shaded
   variants in addition to your existing hbase-client dependency:
    <!-- {x-version-update-start:bigtable-client-parent:released} -->

    ```xml
    Maven:
      ```xml
      <!-- Existing HBase dependency -->
      <dependency>
        <groupId>org.apache.hbase</groupId> 
        <artifactId>hbase-client</artifactId>
        <version>YOUR VERSION OF HBASE</version>
      </dependency>
      <!-- Added bigtable dependency -->
      <dependency>
        <groupId>com.google.cloud.bigtable</groupId>
        <!-- Or bigtable-hbase-1.x-shaded for HBase < 2.0 --> 
        <artifactId>bigtable-hbase-2.x-shaded</artifactId>
        <version>3.0.0</version>
      </dependency>
      ```
    
    Gradle:
      ```Groovy
      // your existing HBase dependency
      compile 'org.apache.hbase:hbase-client:YOUR VERSION OF HBASE'
      // Added Bigtable dependency
      // Or bigtable-hbase-1.x-shaded for HBase < 2.0
      compile 'com.google.cloud.bigtable:bigtable-hbase-2.x-shaded:3.0.0'
      ```
    
    SBT:
    ```Scala
    // your existing HBase dependency
    libraryDependencies += "org.apache.hbase" % "hbase-client" % YOUR VERSION OF HBASE
    // Added Bigtable dependency
    // Or bigtable-hbase-1.x-shaded for HBase < 2.0 
    libraryDependencies += "com.google.cloud.bigtable" % "bigtable-hbase-2.x" % "3.0.0"
    ```
   <!-- {x-version-update-end} -->

2. Change your application to create the HBase Connection to
    ```java
    // Use Bigtable helper to update the HBase configuration object
    Configuration conf = BigtableConfiguration.configure("YOUR PROJECT ID", "YOUR INSTANCE ID");
    // Then create connections as normal using the COnfiguration
    Connection newConnection = ConnectionFactory.createConnection(conf);
    AsyncConnection newAsyncConnection = ConnectionFactory.createAsyncConnection(conf);
    
    ```
#### Option 2:

1. Download the latest -shaded.jar from maven central for your HBase installation:
   <!-- {x-version-start:bigtable-client-parent:released} -->
  *  [bigtable-hbase-1.x-shaded](https://repo1.maven.org/maven2/com/google/cloud/bigtable/bigtable-hbase-1.x-shaded/3.0.0/) for HBase < 2.0
  * [bigtable-hbase-2.x-shaded](https://repo1.maven.org/maven2/com/google/cloud/bigtable/bigtable-hbase-2.x-shaded/3.0.0/) for HBase < 2.4
  <!-- {x-version-update-end} -->
2. Copy the jar into your HBASE_CLASSPATH (ie. hbase-installation/lib)
3. Update your hbase-site.xml:
   For HBase < 2.0:
   ```xml
   <property>
        <name>google.bigtable.project.id</name>
        <value>YOUR_GOOGLE_CLOUD_PROJECT_ID</value>
    </property>
    <property>
        <name>google.bigtable.instance.id</name>
        <value>YOUR_CLOUD_BIGTABLE_INSTANCE_ID</value>
    </property>
   
    <property>
        <name>hbase.client.connection.impl</name>
        <value>com.google.cloud.bigtable.hbase1_x.BigtableConnection</value>
   </property>
   ```
   For HBase < 2.3:
   ```xml
   <property>
        <name>google.bigtable.project.id</name>
        <value>YOUR_GOOGLE_CLOUD_PROJECT_ID</value>
   </property>
   <property>
        <name>google.bigtable.instance.id</name>
        <value>YOUR_CLOUD_BIGTABLE_INSTANCE_ID</value>
   </property>

   <property>
     <name>hbase.client.connection.impl</name>
     <value>com.google.cloud.bigtable.hbase2_x.BigtableConnection</value>
   </property>
   <property>
     <name>hbase.client.async.connection.impl</name>
     <value>org.apache.hadoop.hbase.client.BigtableAsyncConnection</value>
   </property>
   <property>
     <name>hbase.client.registry.impl</name>
     <value>org.apache.hadoop.hbase.client.BigtableAsyncRegistry</value>
   </property>
   ```
   For HBase < 2.4:
   ```xml
   <property>
        <name>google.bigtable.project.id</name>
        <value>YOUR_GOOGLE_CLOUD_PROJECT_ID</value>
   </property>
   <property>
        <name>google.bigtable.instance.id</name>
        <value>YOUR_CLOUD_BIGTABLE_INSTANCE_ID</value>
   </property>

   <property>
     <name>hbase.client.connection.impl</name>
     <value>com.google.cloud.bigtable.hbase2_x.BigtableConnection</value>
   </property>
   <property>
     <name>hbase.client.async.connection.impl</name>
     <value>org.apache.hadoop.hbase.client.BigtableAsyncConnection</value>
   </property>
   <property>
     <name>hbase.client.registry.impl</name>
     <value>org.apache.hadoop.hbase.client.BigtableConnectionRegistry</value>
   </property>
   ```
  



* Refer to the [Connecting to Bigtable](https://cloud.google.com/bigtable/docs/hbase-connecting) documentation for detailed demonstrations of how to configure the properties to connect to Cloud Bigtable.

* Refer to the [Java samples documentation](https://cloud.google.com/bigtable/docs/samples) for detailed demonstrations of how to read and write data with Cloud Bigtable. The code for these samples is available in the [Cloud Bigtable examples project](https://github.com/GoogleCloudPlatform/cloud-bigtable-examples).

## Enabling Client Side Metrics

Cloud Bigtable client supports publishing client side metrics to Cloud Monitoring under the bigtable.googleapis.com/client namespace.

This feature is available once you upgrade to version 2.6.4 and above. Follow the guide on https://cloud.google.com/bigtable/docs/client-side-metrics-setup to enable.

Note: Beam / Dataflow integration is currently not supported.

## Client Metrics

To enable free built-in clientside metrics:

```java
BigtableConfiguration.enableBuiltinMetrics();
```

You can find a description of all the metrics here:
https://cloud.google.com/bigtable/docs/client-side-metrics-descriptions


## Questions and discussions

If you have questions or run into issues with Google Cloud Bigtable or the
client libraries, use any of the following forums:

* Stack Overflow: tag questions with [`google-cloud-bigtable`][stackoverflow-link]
* Mailing list: [google-cloud-bigtable-discuss@][google-cloud-bigtable-discuss]

You can also subscribe to
[google-cloud-bigtable-announce@][google-cloud-bigtable-announce] list to receive
infrequent product and client library announcements.

## Contributing


Contributions to this library are always welcome and highly encouraged.

See [CONTRIBUTING][contributing] for more information how to get started.

Please note that this project is released with a Contributor Code of Conduct. By participating in
this project you agree to abide by its terms. See [Code of Conduct][code-of-conduct] for more
information.

## License

Apache 2.0 - See [LICENSE][license] for more information.

## CI Status

Java Version | Status
------------ | ------
Java 8 | [![Kokoro CI][kokoro-badge-image-1]][kokoro-badge-link-1]
Java 8 OSX | [![Kokoro CI][kokoro-badge-image-2]][kokoro-badge-link-2]
Java 11 | [![Kokoro CI][kokoro-badge-image-3]][kokoro-badge-link-3]
Integration | [![Kokoro CI][kokoro-badge-image-4]][kokoro-badge-link-4]

Java is a registered trademark of Oracle and/or its affiliates.

<!-- references -->

[maven-hbase-shield]: https://maven-badges.herokuapp.com/maven-central/com.google.cloud.bigtable/bigtable-hbase/badge.svg
[maven-hbase-client-maven-search]: http://search.maven.org/#search%7Cga%7C1%7Cg:com.google.cloud.bigtable
[stackoverflow-shield]: https://img.shields.io/badge/stackoverflow-google--cloud--bigtable-blue.svg
[stackoverflow-link]: http://stackoverflow.com/search?q=[google-cloud-bigtable]
[google-cloud-bigtable-discuss]: https://groups.google.com/group/google-cloud-bigtable-discuss
[google-cloud-bigtable-announce]: https://groups.google.com/group/google-cloud-bigtable-announce
[google-cloud-bigtable-emulator]: https://github.com/googleapis/google-cloud-java/tree/main/google-cloud-testing/google-cloud-bigtable-emulator
[google-cloud-bigtable]: https://github.com/googleapis/java-bigtable
[kokoro-badge-image-1]: http://storage.googleapis.com/cloud-devrel-public/java/badges/java-bigtable-hbase/java8.svg
[kokoro-badge-link-1]: http://storage.googleapis.com/cloud-devrel-public/java/badges/java-bigtable-hbase/java8.html
[kokoro-badge-image-2]: http://storage.googleapis.com/cloud-devrel-public/java/badges/java-bigtable-hbase/java8-osx.svg
[kokoro-badge-link-2]: http://storage.googleapis.com/cloud-devrel-public/java/badges/java-bigtable-hbase/java8-osx.html
[kokoro-badge-image-3]: http://storage.googleapis.com/cloud-devrel-public/java/badges/java-bigtable-hbase/java11.svg
[kokoro-badge-link-3]: http://storage.googleapis.com/cloud-devrel-public/java/badges/java-bigtable-hbase/java11.html
[kokoro-badge-image-4]: http://storage.googleapis.com/cloud-devrel-public/java/badges/java-bigtable-hbase/integration.svg
[kokoro-badge-link-4]: http://storage.googleapis.com/cloud-devrel-public/java/badges/java-bigtable-hbase/integration.html
[contributing]: https://github.com/googleapis/java-bigtable-hbase/blob/main/CONTRIBUTING.md
[code-of-conduct]: https://github.com/googleapis/java-bigtable-hbase/blob/main/CODE_OF_CONDUCT.md#contributor-code-of-conduct
[license]: https://github.com/googleapis/java-bigtable-hbase/blob/main/LICENSE
