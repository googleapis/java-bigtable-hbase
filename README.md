## Google Cloud Bigtable Client

Bigger than a data warehouse, fast enough for real-time access, and less expensive than running virtual machines. The world-renowned database that powers Google is now available to you worldwide.

### Overview

Google Cloud Bigtable offers you a fast, fully managed, almost infinitely scalable NoSQL database service that's ideal for web, mobile, and IoT applications requiring terabytes to petabytes of data. Unlike comparable market offerings, Cloud Bigtable doesn't require you to sacrifice speed, scale, or cost efficiency when your applications grow. Cloud Bigtable has been battle-tested at Google for more than 10 years—it's the database driving major applications such as Google Analytics and Gmail.

### Fully Managed

Cloud Bigtable is a high-availability, fully managed NoSQL database service. Google's experienced engineers work around the clock to keep your Cloud Bigtable servers up and running. With Cloud Bigtable, you'll spend more time developing valuable applications and less time architecting solutions that scale.

### Fully Integrated

Data can be imported to or exported from Cloud Bigtable through simple bulk ingestion tools. You can transition to Cloud Bigtable from privately managed Apache HBase instances, and it's easy to export Cloud Bigtable data to an industry-standard format.

### Fast and Highly Scalable

Cloud Bigtable scales to hundreds of petabytes seamlessly and automatically. It delivers low response times to support real-time data ingestion.

### Industry Standard

Cloud Bigtable is offered through the standard Apache HBase open-source API with simple bulk ingestion and export.

### Protected

Your data is protected through redundant storage at multiple physical locations. OAuth and granular access controls provide strong, configurable security.

### Proven

Cloud Bigtable has been used by Google for 10+ years for its largest applications, including search, Gmail, Google Maps, Google Earth, and many others.


## Project setup, installation, and configuration

There are a few things you need to do to get a working Bigtable project before you can execute code against it:

1. [Create a Cloud Bigtable cluster](https://cloud.google.com/bigtable/docs/creating-cluster)
1. It's recommended to [install the Cloud SDK for Cloud Bigtable](https://cloud.google.com/bigtable/docs/installing-cloud-sdk)

You can create a [Maven project](https://cloud.google.com/bigtable/docs/using-maven) with the following dependency:

```xml
<dependency>
  <groupId>com.google.cloud.bigtable</groupId>
  <artifactId>bigtable-hbase-1.0</artifactId>
  <version>0.9.1</version>
</dependency>

<dependency>
  <groupId>io.netty</groupId>
  <artifactId>netty-tcnative-boringssl-static</artifactId>
  <version>1.1.33.Fork19</version>
</dependency>
```

### How do I, as a developer, start working on the project?

You'll need Java, Maven, Google Cloud Platform project with a Bigtable cluster (see above for details) and the Google Cloud SDK (see above).

You can run `mvn clean install` to create a local version of all of the Cloud Bigtable client binaries. 

Additionally, see our [example project](https://github.com/GoogleCloudPlatform/cloud-bigtable-examples).

## Testing

You can run the unit tests via `mvn clean install`.  There is also a suite of integration tests that connect to a cluster you have access to via the authentication credentials that were loaded via the cloud SDK configuration.

You can run the following command for Java 7:

    mvn clean integration-test \
        -PbigtableIntegrationTest \
        -Dgoogle.bigtable.project.id=[your cloud project id] \
        -Dgoogle.bigtable.instance.id=[your cloud bigtable instance id] \

## Deploying
**TODO**

### How to setup the deployment environment
**TODO**


### How to deploy
**TODO**


## Troubleshooting & useful tools
**TODO**

### Examples of common tasks
**TODO**

## Contributing changes

* See [CONTRIBUTING.md](CONTRIBUTING.md)


## Licensing

* See [LICENSE](LICENSE)
