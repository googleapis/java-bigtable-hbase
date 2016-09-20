# Bigtable Protos

## Purpose
This project houses the raw .proto files that are used by bigtable-legacy-compatibility, which is primarily geared towards v1 protobuf objects used by Google Cloud Dataflow.  This project generates a jar with java artifacts derived from the .proto files in this project.  These .proto files can also be used to generate other clients.

This project relies on artifacts from https://github.com/grpc/grpc-java. You can see more about gRPC at https://github.com/grpc/grpc-common.

## Bigtable .proto files

Primarily, Google Cloud Dataflow uses Mutation, Row and RowFilter as the top level objects that users would manipulate.  There are some objects that are used by those top level objects, as well as some objects that are not used by Google Cloud Dataflow, but are still included for the simplicity of managing this project.

Here are the files directly associated with Bigtable functionality 
* src/main/proto/google/bigtable/v1/bigtable_data.proto

### Copied Protos ###

There were some .proto files copied into this project from other projects in order to have a full set of dependencies required to generating Java artifacts from the bigtable v1 .protos.  The generated artifacts are duplicate of objects in the bigtable-proto project, so they are removed after they are generated.

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
mvn -P generateProtos package && find . -name '*.java' | grep -v bigtable | xargs rm
```

