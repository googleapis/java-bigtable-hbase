# [Google Cloud Bigtable HBase client for Java](https://cloud.google.com/bigtable/docs/bigtable-and-hbase)

[![Travis CI status][travis-shield]][travis-link]
[![Maven][maven-shield]][maven-link]
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

## Project setup, installation, and configuration

### Prerequisites

* [Create a Cloud Bigtable instance](https://cloud.google.com/bigtable/docs/creating-instance)
* **Recommended**: [Install the Google Cloud SDK for Cloud Bigtable](https://cloud.google.com/bigtable/docs/installing-cloud-sdk)

### Using the Java client

* Add the appropriate [Cloud Bigtable artifact dependencies](http://mvnrepository.com/artifact/com.google.cloud.bigtable) to your [Maven project](https://cloud.google.com/bigtable/docs/using-maven).
  * bigtable-hbase-1.x: use for standalone applications where you are in control your dependencies.
  * bigtable-hbase-1.x-hadoop: use in hadoop environments
  * bigtable-hbase-1.x-shaded: use in environments (other than hadoop) that require older versions of protobuf, guava, etc.  

* Example:
   ```xml
    <dependency>
        <groupId>com.google.cloud.bigtable</groupId>
        <artifactId>bigtable-hbase-1.x</artifactId>
        <version>1.0.0</version>
    </dependency>
  ```

* Refer to the [Java samples documentation](https://cloud.google.com/bigtable/docs/samples) for detailed demonstrations of how to read and write data with Cloud Bigtable. The code for these samples is available in the [Cloud Bigtable examples project](https://github.com/GoogleCloudPlatform/cloud-bigtable-examples).

## Questions and discussions

If you have questions or run into issues with Google Cloud Bigtable or the
client libraries, you can use any of the following forums:

* Stack Overflow: tag questions with [`google-cloud-bigtable`][stackoverflow-link]
* Mailing list: [google-cloud-bigtable-discuss@][google-cloud-bigtable-discuss]

You can also subscribe to
[google-cloud-bigtable-announce@][google-cloud-bigtable-announce] list to receive
infrequent product and client library announcements.

## Developing and testing

1. Running `mvn clean install` will build and install Cloud Bigtable client artifacts to your local repository, and then run the unit tests.
2. There is also a suite of integration tests that connect to a cluster you have access to via the authentication credentials that were loaded via the Google Cloud SDK configuration step (see above).

   Use the following command to run the integration tests:

   ```sh
   mvn clean integration-test \
       -PbigtableIntegrationTest \
       -Dgoogle.bigtable.project.id=[your cloud project id] \
       -Dgoogle.bigtable.instance.id=[your cloud bigtable instance id]
   ```

NOTE: This project uses extensive shading which IDEs have trouble with. To over come these issues,
you can disable the `with-shaded` profile in your IDE to force it to resolve the dependencies from your local
maven repository. When you disable that profile, you have to attach workspace sources to local maven repository jars.

## Contributing changes

See [CONTRIBUTING.md](CONTRIBUTING.md) for more information on how to contribute
to this project.

## License

Apache 2.0; see [LICENSE](LICENSE) for details.

<!-- references -->

[travis-shield]: https://travis-ci.org/GoogleCloudPlatform/cloud-bigtable-client.svg
[travis-link]: https://travis-ci.org/GoogleCloudPlatform/cloud-bigtable-client/builds
[maven-shield]: https://maven-badges.herokuapp.com/maven-central/com.google.cloud.bigtable/bigtable-client-core/badge.svg
[maven-link]: http://search.maven.org/#search%7Cga%7C1%7Ccom.google.cloud.bigtable
[stackoverflow-shield]: https://img.shields.io/badge/stackoverflow-google--cloud--bigtable-blue.svg
[stackoverflow-link]: http://stackoverflow.com/search?q=[google-cloud-bigtable]
[integrations]: https://cloud.google.com/bigtable/docs/integrations
[google-cloud-bigtable-discuss]: https://groups.google.com/group/google-cloud-bigtable-discuss
[google-cloud-bigtable-announce]: https://groups.google.com/group/google-cloud-bigtable-announce
