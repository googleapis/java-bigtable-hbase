# [Google Cloud Bigtable HBase client for Java](https://cloud.google.com/bigtable/docs/bigtable-and-hbase)

[![build.status](https://travis-ci.org/GoogleCloudPlatform/cloud-bigtable-client.svg)](https://travis-ci.org/GoogleCloudPlatform/cloud-bigtable-client/builds) [![ghit.me](https://ghit.me/badge.svg?repo=GoogleCloudPlatform/cloud-bigtable-client)](https://ghit.me/repo/GoogleCloudPlatform/cloud-bigtable-client)
[![maven.version](https://maven-badges.herokuapp.com/maven-central/com.google.cloud.bigtable/bigtable-client-core/badge.svg)](http://search.maven.org/#search%7Cga%7C1%7Ccom.google.cloud.bigtable)

Bigger than a data warehouse, fast enough for real-time access, and less expensive than running virtual machines. The world-renowned database that powers Google is now available to you worldwide.

## Overview

[Google Cloud Bigtable](https://cloud.google.com/bigtable/) offers you a fast, fully managed, almost infinitely scalable NoSQL database service that's ideal for web, mobile, and IoT applications requiring terabytes to petabytes of data. Unlike comparable market offerings, Cloud Bigtable doesn't require you to sacrifice speed, scale, or cost efficiency when your applications grow. The Bigtable service that Cloud Bigtable relies upon has been battle-tested at Google for more than 10 years—it's the database driving major applications such as Google Analytics and Gmail.

## Project setup, installation, and configuration

### Prerequisites

* [Create a Cloud Bigtable cluster](https://cloud.google.com/bigtable/docs/creating-cluster)
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

## Testing
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

* See [CONTRIBUTING.md](CONTRIBUTING.md)

## Licensing

* See [LICENSE](LICENSE)
