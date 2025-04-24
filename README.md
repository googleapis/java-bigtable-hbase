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

**Note**: These artifacts are meant to wrap HBase over the Bigtable API. If you are looking for a Java client to access Bigtable APIs directly, please use [google-cloud-bigtable][google-cloud-bigtable].

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
    <version>2.6.5</version>
  </dependency>
  ```

  Gradle:
  ```Groovy
  compile 'com.google.cloud.bigtable:bigtable-hbase-1.x:2.6.5'
  ```

  SBT:
  ```Scala
  libraryDependencies += "com.google.cloud.bigtable" % "bigtable-hbase-1.x" % "2.6.5"
  ```
[//]: # ({x-version-update-end})

* Refer to the [Connecting to Bigtable](https://cloud.google.com/bigtable/docs/hbase-connecting) documentation for detailed demonstrations of how to configure the properties to connect to Cloud Bigtable.

* Refer to the [Java samples documentation](https://cloud.google.com/bigtable/docs/samples) for detailed demonstrations of how to read and write data with Cloud Bigtable. The code for these samples is available in the [Cloud Bigtable examples project](https://github.com/GoogleCloudPlatform/cloud-bigtable-examples).

## Enabling Client Side Metrics

Cloud Bigtable client supports publishing client side metrics to Cloud Monitoring under the bigtable.googleapis.com/client namespace.

This feature is available once you upgrade to version 2.6.4 and above. Follow the guide on https://cloud.google.com/bigtable/docs/client-side-metrics-setup to enable.

Note: Beam / Dataflow integration is currently not supported.

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
[integrations]: https://cloud.google.com/bigtable/docs/integrations
[maven-examples-repo]: https://github.com/GoogleCloudPlatform/cloud-bigtable-examples
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
