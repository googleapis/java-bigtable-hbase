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
**Include authorship, support contact and release information.**


## Project setup, installation, and configuration
**TODO**

## Building

### grpc-java and Netty
Cloud-bigtable requires grpc-java and its dependencies.
```
$ git submodule update --init --recursive
$ cd lib/grpc-java/
```
At this point, follow the directions to install grpc-java via the [README](https://github.com/grpc/grpc-java/blob/master/README.md)

### bigtable-client
```
$ mvn clean install -DskipTests
```

### Examples
Additionally see our [example project](https://github.com/GoogleCloudPlatform/cloud-bigtable-examples)

## Testing

How do I run the project's automated tests?

* Unit Tests
mvn package

* Integration Tests
**TODO**

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
