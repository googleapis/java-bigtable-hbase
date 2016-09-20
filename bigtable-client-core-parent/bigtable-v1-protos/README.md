# Bigtable Protos

## Purpose
This project houses the raw .proto files that are used for accessing Google Cloud Bigtable.  This project generates a jar with java artifacts derived from the proto files in this project.  These proto files can also be used to generate other clients.

This project relies on artifacts from https://github.com/grpc/grpc-java. You can see more about gRPC at https://github.com/grpc/grpc-common.

## Bigtable proto files

There are 3 types of functionality that the protos support:

* Table administration - creating, modifying and deleting Bigtable tables
* Table data manipulation
* Cluster administration - creating, modifying and delting Bigtable clusters, which are a collection of Tables.

Here are the files directly associated with Bigtable functionality 
* src/main/proto/google/bigtable/v1/bigtable_data.proto

Copied Protos
--------------

There were some proto files copied into this project from other projects.

Retrieved from https://github.com/google/googleapis:

* src/main/proto/google/api/annotations.proto
* src/main/proto/google/api/http.proto
* src/main/proto/google/longrunning/operations.proto
* src/main/proto/google/rpc/code.proto
* src/main/proto/google/rpc/error_details.proto
* src/main/proto/google/rpc/status.proto
* src/main/proto/google/type/color.proto
* src/main/proto/google/type/date.proto
* src/main/proto/google/type/dayofweek.proto
* src/main/proto/google/type/timeofday.proto

# Generating the java gRPC implementations

Run the following:

```
mvn -P generateProtos package
find . -name '*.java' | grep -v bigtable | xargs rm
```

# Generating the Python protos

The gRPC Python generation process requires protoc 3

Protoc can be installed via the instructions at:
https://github.com/grpc/grpc-java#build-protobuf.
Installing it isn't always trivial;
you may have to Google error messages to understand what to install next;
there are plenty of answers on StackOverflow for protobuf installation requirements.  

Check the installation with the following command: `protoc --version`.

After that, run the following:

```bash
$ cd bigtable-protos
$ mkdir -p src/generated/python
$ cd src/main/proto
$ protoc google/bigtable/v1/*.proto --python_out=../../generated/python
```

Your generated files (`*_pb2.py`) should be inside `src/generated/python`.
