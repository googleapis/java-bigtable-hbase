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

[//]: # ({x-version-update-start:bigtable-hbase-1x-parent:released})
  Maven:
  ```xml
  <dependency>
    <groupId>com.google.cloud.bigtable</groupId>
    <artifactId>bigtable-hbase-1.x</artifactId>
    <version>1.15.0</version>
  </dependency>
  ```

  Gradle:
  ```Groovy
  compile 'com.google.cloud.bigtable:bigtable-hbase-1.x:1.15.0'
  ```

  SBT:
  ```Scala
  libraryDependencies += "com.google.cloud.bigtable" % "bigtable-hbase-1.x" % "1.15.0"
  ```
[//]: # ({x-version-update-end})
* Refer to the [Java samples documentation](https://cloud.google.com/bigtable/docs/samples) for detailed demonstrations of how to read and write data with Cloud Bigtable. The code for these samples is available in the [Cloud Bigtable examples project](https://github.com/GoogleCloudPlatform/cloud-bigtable-examples).

## Questions and discussions

If you have questions or run into issues with Google Cloud Bigtable or the
client libraries, use any of the following forums:

* Stack Overflow: tag questions with [`google-cloud-bigtable`][stackoverflow-link]
* Mailing list: [google-cloud-bigtable-discuss@][google-cloud-bigtable-discuss]

You can also subscribe to
[google-cloud-bigtable-announce@][google-cloud-bigtable-announce] list to receive
infrequent product and client library announcements.

## Clients and Repositories

| Name | Language | Repository | latest version | status |
| --- | --- | --- | --- | --- |
| Cloud Bigtable Examples | Java, others | [GoogleCloudPlatform/cloud-bigtable-examples][maven-examples-repo] | | |
| HBase client | Java | [googleapis/cloud-bigtable-client][maven-hbase-client-repo] | [![Maven][maven-hbase-shield]][maven-hbase-client-maven-search] | GA |
| Cloud Bigtable GoLang | Go | [googleapis/google-cloud-go](https://github.com/googleapis/google-cloud-go) | N/A | GA |
| Cloud Bigtable Java | Java | [googleapis/java-bigtable][google-cloud-bigtable] | [![Maven][maven-google-cloud-java-shield]][maven-google-cloud-java-maven-search] | GA |
| Cloud Bigtable Python | Python | [googleapis/google-cloud-python](http://github.com/googleapis/google-cloud-python) | [![PyPI version](https://badge.fury.io/py/google-cloud-bigtable.svg)](https://badge.fury.io/py/google-cloud-bigtable) | Beta |
| Cloud Bigtable Node.js | Node | [googleaps/nodejs-bigtable][maven-bigtable-nodejs-repo] | [![npm version][npm-shield]][npm-bigtable-client] | GA |
| Cloud Bigtable C# | C# | [googleapis/google-cloud-dotnet](http://github.com/googleapis/google-cloud-dotnet) | [![NuGet version](https://badge.fury.io/nu/Google.Cloud.Bigtable.V2.svg)](https://badge.fury.io/nu/Google.Cloud.Bigtable.V2) | GA |
| Cloud Bigtable C++ | C++ | [googleapis/google-cloud-cpp](http://github.com/googleapis/google-cloud-cpp) | 0.9.0 | Beta |
| Cloud Bigtable Ruby | Ruby | [googleapis/google-cloud-ruby](http://github.com/googleapis/google-cloud-ruby) | [![Gem Version](https://badge.fury.io/rb/google-cloud-bigtable.svg)](https://badge.fury.io/rb/google-cloud-bigtable) | Beta |
| Cloud Bigtable PHP | PHP | [googleapis/google-cloud-php](http://github.com/googleapis/google-cloud-php) | [![Latest Stable Version](https://poser.pugx.org/google/cloud/v/stable)](https://packagist.org/packages/google/cloud) | Beta  |

## Contributing changes

See [CONTRIBUTING.md](CONTRIBUTING.md) for more information on how to contribute
to this project.

## License

Apache 2.0; see [LICENSE](LICENSE) for details.

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
