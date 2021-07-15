# [Google Cloud Bigtable HBase client for Java](https://cloud.google.com/bigtable/docs/bigtable-and-hbase)

[![Maven][maven-hbase-shield]][maven-hbase-client-maven-search]
[![Stack Overflow][stackoverflow-shield]][stackoverflow-link]

[Google Cloud Bigtable](https://cloud.google.com/bigtable/) is Google's NoSQL
Big Data database service. It's the same database that powers many core Google
services, including Search, Analytics, Maps, and Gmail.

Bigtable is designed to handle massive workloads at consistent low latency and
high throughput, so it's a great choice for both operational and analytical
applications, including IoT, user analytics, and financial data analysis.

Bigtable provisions and scales to hundreds of petabytes automatically, and can
smoothly handle millions of operations per second. Changes to the deployment
configuration are immediate, so there is no downtime during reconfiguration.

Bigtable [integrates easily][integrations] with popular Big Data tools like
Hadoop, as well as Google Cloud Platform products like Cloud Dataflow and
Dataproc. Plus, Bigtable supports the open-source, industry-standard HBase API,
which makes it easy for development teams to get started.

Note: Please use [google-cloud-bigtable][google-cloud-bigtable] to access Bigtable APIs instead of `bigtable-core-client`. These artifacts are meant to wrap HBase over Bigtable API. 

## Project setup, installation, and configuration

### Prerequisites

* [Create a Cloud Bigtable instance](https://cloud.google.com/bigtable/docs/creating-instance)
* **Recommended**: [Install the Google Cloud SDK for Cloud Bigtable](https://cloud.google.com/bigtable/docs/installing-cloud-sdk)

### Using the Java client

* Add the appropriate [Cloud Bigtable artifact dependencies](http://mvnrepository.com/artifact/com.google.cloud.bigtable) to your [Maven project](https://cloud.google.com/bigtable/docs/using-maven).
  * `bigtable-hbase-1.x`: use for standalone applications where you are in control of your dependencies.
  * `bigtable-hbase-1.x-hadoop`: use in hadoop environments.
  * `bigtable-hbase-1.x-mapreduce`: use for map/reduce utilities.
  * `bigtable-hbase-1.x-shaded`: use in environments (other than hadoop) that require older versions of protobuf, guava, etc.  
  * `bigtable-hbase-2.x`: use for standalone applications where you are in control of your dependencies.  This includes an HBase async client.
  * `bigtable-hbase-2.x-hadoop`: use in hadoop environments.
  * `bigtable-hbase-2.x-shaded`: use in environments (other than hadoop) that require older versions of protobuf, guava, etc.  

[//]: # ({x-version-update-start:bigtable-client-parent:released})
  Maven:
  ```xml
  <dependency>
    <groupId>com.google.cloud.bigtable</groupId>
    <artifactId>bigtable-hbase-1.x</artifactId>
    <version>1.23.0</version>
  </dependency>
  ```

  Gradle:
  ```Groovy
  compile 'com.google.cloud.bigtable:bigtable-hbase-1.x:1.23.0'
  ```

  SBT:
  ```Scala
  libraryDependencies += "com.google.cloud.bigtable" % "bigtable-hbase-1.x" % "1.23.0"
  ```
[//]: # ({x-version-update-end})
* Refer to the [Java samples documentation](https://cloud.google.com/bigtable/docs/samples) for detailed demonstrations of how to read and write data with Cloud Bigtable. The code for these samples is available in the [Cloud Bigtable examples project](https://github.com/GoogleCloudPlatform/cloud-bigtable-examples).

## OpenCensus Integration

The Bigtable HBase Client supports OpenCensus telemetry, specifically exporting gRPC metrics to Stats and supporting
Tracing.

### Stats

The code example below shows how to enable metrics. For more details, see the [gRPC Java Guide](https://opencensus.io/guides/grpc/java/).

##### Maven Setup

If you are _not_ using the shaded Bigtable HBase Client artifact, you need to define the OpenCensus dependencies.

[//]: # ({x-version-update-start:bigtable-client-parent:released})
```xml
<!-- OpenCensus dependencies -->
<dependency>
    <groupId>com.google.cloud.bigtable</groupId>
    <artifactId>bigtable-hbase-1.x</artifactId>
    <version>1.23.0</version>
</dependency>
<dependency>
    <groupId>io.opencensus</groupId>
    <artifactId>opencensus-impl</artifactId>
    <version>0.24.0</version>
    <scope>runtime</scope>
</dependency>
<dependency>
    <groupId>io.opencensus</groupId>
    <artifactId>opencensus-exporter-stats-stackdriver</artifactId>
    <version>0.24.0</version>
    <exclusions>
        <exclusion>
            <groupId>io.grpc</groupId>
            <artifactId>*</artifactId>
        </exclusion>
        <exclusion>
            <groupId>com.google.auth</groupId>
            <artifactId>*</artifactId>
        </exclusion>
    </exclusions>
</dependency>
```

If you _are_ using the shaded Bigtable HBase Client artifact, then the OpenCensus dependencies are embedded in the
shaded artifact; i.e. nothing additional for you to do.

```xml
<!-- OpenCensus dependencies -->
<dependency>
    <groupId>com.google.cloud.bigtable</groupId>
    <artifactId>bigtable-hbase-1.x-shaded</artifactId>
    <version>1.23.0</version>
</dependency>
```
[//]: # ({x-version-update-end})

##### Java Example
```java
// For the non-shaded client, remove the package prefix "com.google.bigtable.repackaged."
import com.google.bigtable.repackaged.io.opencensus.contrib.grpc.metrics.RpcViews;
import com.google.bigtable.repackaged.io.opencensus.exporter.stats.stackdriver.StackdriverStatsConfiguration;
import com.google.bigtable.repackaged.io.opencensus.exporter.stats.stackdriver.StackdriverStatsExporter;

import java.io.IOException;

public class OpenCensusExample {
    String projectId = "your-project-id";

    void setupStatsExport() throws Exception {
        // Option 1: Automatic Configuration (from GCP Resources only):
        // If you are running from a GCP Resource (e.g. a GCE VM), the Stackdriver metrics are automatically
        // configured to upload to your resource.
        // For examples of monitored resources, see here: https://cloud.google.com/monitoring/api/resources
        StackdriverStatsExporter.createAndRegister();

        // Then register your gRPC views in OpenCensus.
        RpcViews.registerClientGrpcViews();


        // Option 2: Manual Configuration
        // If you are not running from a GCP Resource (e.g. if you are running on-prem), then you should
        // configure the monitored resource yourself.
        // Use the code snippet below as a starting example.
        // For examples of monitored resources, see here: https://cloud.google.com/monitoring/api/resources
        StackdriverStatsExporter.createAndRegister(
                StackdriverStatsConfiguration.builder()
                        .setProjectId(projectId)
                        // This example uses generic_node as the MonitoredResource, with your host name as the node ID.
                        .setMonitoredResource(MonitoredResource.newBuilder()
                                .setType("generic_node")
                                .putLabels("project_id", projectId)
                                .putLabels("location", "us-west1-b")  // Specify the region in which your service is running (e.g. us-west1-b). 
                                .putLabels("namespace", "anyNamespaceYouChoose")
                                .putLabels("node_id", InetAddress.getLocalHost().getHostName())  // Specify any node you choose (e.g. the local hostname).
                                .build())
                        .build()
        );
        
        // Then register your gRPC views in OpenCensus.
        RpcViews.registerClientGrpcViews();
    }
}
```

##### Viewing Your Metrics in Google Cloud Console

The above steps will expose Bigtable's gRPC metrics under the
custom.googleapis.com/opencensus/grpc.io/client prefix.

Follow [these instructions](https://opencensus.io/guides/grpc/java/) for viewing the metrics in
Google Cloud Console.

Be sure to choose your Resource Type as the one you defined in your Stackdriver configuration in
the code.


### Tracing

The code example below shows how to enable tracing. For more details, see [here](https://cloud.google.com/community/tutorials/bigtable-oc).

##### Maven Setup

If you are _not_ using the shaded Bigtable HBase Client artifact, you need to define the OpenCensus dependencies.

[//]: # ({x-version-update-start:bigtable-client-parent:released})
```xml
<!-- OpenCensus dependencies -->
<dependency>
    <groupId>com.google.cloud.bigtable</groupId>
    <artifactId>bigtable-hbase-1.x</artifactId>
    <version>1.23.0</version>
</dependency>
<dependency>
    <groupId>io.opencensus</groupId>
    <artifactId>opencensus-impl</artifactId>
    <version>0.24.0</version>
    <scope>runtime</scope>
</dependency>
<dependency>
    <groupId>io.opencensus</groupId>
    <artifactId>opencensus-exporter-trace-stackdriver</artifactId>
    <version>0.24.0</version>
    <exclusions>
        <exclusion>
            <groupId>io.grpc</groupId>
            <artifactId>*</artifactId>
        </exclusion>
        <exclusion>
            <groupId>com.google.auth</groupId>
            <artifactId>*</artifactId>
        </exclusion>
    </exclusions>
</dependency>
```

If you _are_ using the shaded Bigtable HBase Client artifact, then the OpenCensus dependencies are embedded in the
shaded artifact; i.e. nothing additional for you to do.

```xml
<!-- OpenCensus dependencies -->
<dependency>
    <groupId>com.google.cloud.bigtable</groupId>
    <artifactId>bigtable-hbase-1.x-shaded</artifactId>
    <version>1.23.0</version>
</dependency>
```
[//]: # ({x-version-update-end})

##### Java Example

```java
// For the non-shaded client, remove the package prefix "com.google.bigtable.repackaged."
import com.google.bigtable.repackaged.io.opencensus.exporter.trace.stackdriver.StackdriverTraceConfiguration;
import com.google.bigtable.repackaged.io.opencensus.exporter.trace.stackdriver.StackdriverTraceExporter;
import com.google.bigtable.repackaged.io.opencensus.trace.Tracing;
import com.google.bigtable.repackaged.io.opencensus.trace.samplers.Samplers;

import java.io.IOException;

public class OpenCensusExample {
    String projectId = "your-project-id";

    void setupTracing() throws Exception {
        // Setup tracing.
        StackdriverTraceExporter.createAndRegister(
                StackdriverTraceConfiguration.builder()
                        .setProjectId(projectId)
                        .build()
        );
        Tracing.getTraceConfig().updateActiveTraceParams(
                Tracing.getTraceConfig().getActiveTraceParams().toBuilder()
                        // Adjust the sampling rate as you see fit.
                        .setSampler(Samplers.probabilitySampler(0.01))
                        .build()
        );
    }
}
```

## Questions and discussions

If you have questions or run into issues with Google Cloud Bigtable or the
client libraries, use any of the following forums:

* Stack Overflow: tag questions with [`google-cloud-bigtable`][stackoverflow-link]
* Mailing list: [google-cloud-bigtable-discuss@][google-cloud-bigtable-discuss]

You can also subscribe to
[google-cloud-bigtable-announce@][google-cloud-bigtable-announce] list to receive
infrequent product and client library announcements.

## Contributing changes

See [CONTRIBUTING.md](CONTRIBUTING.md) for more information on how to contribute
to this project.

## License

Apache 2.0; see [LICENSE](LICENSE) for details.

## CI Status

Java Version | Status
------------ | ------
Java 8 | [![Kokoro CI][kokoro-badge-image-1]][kokoro-badge-link-1]
Java 8 OSX | [![Kokoro CI][kokoro-badge-image-2]][kokoro-badge-link-2]
Java 11 | [![Kokoro CI][kokoro-badge-image-3]][kokoro-badge-link-3]
Integration | [![Kokoro CI][kokoro-badge-image-4]][kokoro-badge-link-4]

Java is a registered trademark of Oracle and/or its affiliates.

<!-- references -->

[maven-hbase-shield]: https://maven-badges.herokuapp.com/maven-central/com.google.cloud.bigtable/bigtable-client-core/badge.svg
[maven-hbase-client-maven-search]: http://search.maven.org/#search%7Cga%7C1%7Ccom.google.cloud.bigtable
[maven-google-cloud-java-shield]: https://maven-badges.herokuapp.com/maven-central/com.google.cloud/google-cloud-bigtable/badge.svg
[maven-google-cloud-java-maven-search]: https://search.maven.org/search?q=a:google-cloud-bigtable
[npm-shield]: https://badge.fury.io/js/%40google-cloud%2Fbigtable.svg
[npm-bigtable-client]: https://www.npmjs.com/package/@google-cloud/bigtable
[stackoverflow-shield]: https://img.shields.io/badge/stackoverflow-google--cloud--bigtable-blue.svg
[stackoverflow-link]: http://stackoverflow.com/search?q=[google-cloud-bigtable]
[integrations]: https://cloud.google.com/bigtable/docs/integrations
[maven-hbase-client-repo]: https://github.com/googleapis/cloud-bigtable-client
[maven-bigtable-nodejs-repo]: https://github.com/googleapis/nodejs-bigtable
[maven-examples-repo]: https://github.com/GoogleCloudPlatform/cloud-bigtable-examples
[google-cloud-bigtable-discuss]: https://groups.google.com/group/google-cloud-bigtable-discuss
[google-cloud-bigtable-announce]: https://groups.google.com/group/google-cloud-bigtable-announce
[google-cloud-bigtable-emulator]: https://github.com/googleapis/google-cloud-java/tree/master/google-cloud-testing/google-cloud-bigtable-emulator
[google-cloud-bigtable]: https://github.com/googleapis/java-bigtable
[kokoro-badge-image-1]: http://storage.googleapis.com/cloud-devrel-public/java/badges/java-bigtable-hbase/java8.svg
[kokoro-badge-link-1]: http://storage.googleapis.com/cloud-devrel-public/java/badges/java-bigtable-hbase/java8.html
[kokoro-badge-image-2]: http://storage.googleapis.com/cloud-devrel-public/java/badges/java-bigtable-hbase/java8-osx.svg
[kokoro-badge-link-2]: http://storage.googleapis.com/cloud-devrel-public/java/badges/java-bigtable-hbase/java8-osx.html
[kokoro-badge-image-3]: http://storage.googleapis.com/cloud-devrel-public/java/badges/java-bigtable-hbase/java11.svg
[kokoro-badge-link-3]: http://storage.googleapis.com/cloud-devrel-public/java/badges/java-bigtable-hbase/java11.html
[kokoro-badge-image-4]: http://storage.googleapis.com/cloud-devrel-public/java/badges/java-bigtable-hbase/integration.svg
[kokoro-badge-link-4]: http://storage.googleapis.com/cloud-devrel-public/java/badges/java-bigtable-hbase/integration.html
