# [Google Cloud Bigtable HBase client for Java](https://cloud.google.com/bigtable/docs/bigtable-and-hbase)

[![Travis CI status][travis-shield]][travis-link]
[![Maven][maven-shield]][maven-link]
[![Stack Overflow][stackoverflow-shield]][stackoverflow-link]

Bigger than a data warehouse, fast enough for real-time access, and less expensive than running virtual machines. The world-renowned database that powers Google is now available to you worldwide.

## Overview

[Google Cloud Bigtable](https://cloud.google.com/bigtable/) offers you a fast, fully managed, almost infinitely scalable NoSQL database service that's ideal for web, mobile, and IoT applications requiring terabytes to petabytes of data. Unlike comparable market offerings, Cloud Bigtable doesn't require you to sacrifice speed, scale, or cost efficiency when your applications grow. The Bigtable service that Cloud Bigtable relies upon has been battle-tested at Google for more than 10 years—it's the database driving major applications such as Search, Analytics, Maps and Gmail.

## Project setup, installation, and configuration

### Prerequisites

* [Create a Cloud Bigtable instance](https://cloud.google.com/bigtable/docs/creating-instance)
* **Recommended**: [Install the Google Cloud SDK for Cloud Bigtable](https://cloud.google.com/bigtable/docs/installing-cloud-sdk)

### Using the Java client

* Add the appropriate [Cloud Bigtable artifact dependencies](http://mvnrepository.com/artifact/com.google.cloud.bigtable) to your [Maven project](https://cloud.google.com/bigtable/docs/using-maven), e.g.:
 ```xml
  <dependency>
      <groupId>com.google.cloud.bigtable</groupId>
      <artifactId>bigtable-hbase-1.2</artifactId>
      <version>0.9.1</version>
  </dependency>

  <dependency>
      <groupId>io.netty</groupId>
      <artifactId>netty-tcnative-boringssl-static</artifactId>
      <version>1.1.33.Fork19</version>
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
1. There is also a suite of integration tests that connect to a cluster you have access to via the authentication credentials that were loaded via the Google Cloud SDK configuration step (see above).

   Use the following command to run the integration tests:

   ```sh
   mvn clean integration-test \
       -PbigtableIntegrationTest \
       -Dgoogle.bigtable.project.id=[your cloud project id] \
       -Dgoogle.bigtable.instance.id=[your cloud bigtable instance id]
   ```

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
[google-cloud-bigtable-discuss]: https://groups.google.com/group/google-cloud-bigtable-discuss
[google-cloud-bigtable-announce]: https://groups.google.com/group/google-cloud-bigtable-announce
