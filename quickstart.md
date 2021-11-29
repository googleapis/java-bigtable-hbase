# Mirroring client
## High-level overview
The aim of this project is to provide a drop-in replacement for the HBase client that mirrors operations performed on the HBase cluster to the Bigtable cluster to facilitate migration from HBase to Bigtable.

The client connects two databases, called a primary and a secondary. 
By default operations are performed on the primary database and successful ones are replayed on the secondary asynchronously (this behaviour is configurable).
The client does a best-effort attempt to keep the databases in sync, however, it does not ensure consistency. 
When a write to the secondary database fails it is (depending on the mode, described below) written to a log on disk so the user can replay it manually later, or thrown as an exception to the user.
The consistency of both databases is verified when reads are performed. A fraction of reads is replayed on the secondary database and their content is compared -  mismatches are reported as a log message.
Handling of write errors and read mismatches can be overridden by the user.

HBaseAdmin is not supported.

## Example configuration
The Mirroring Client reads its configuration from default hbase configuration store (by default, `hbase-size.xml` file). In the simplest case the user just have to merge their HBase and bigtable-hbase-java configurations into a single file and correctly set `hbase.client.connection.impl`, `google.bigtable.mirroring.primary-client.connection.impl` and `google.bigtable.mirroring.secondary-client.connection.impl` keys as shown in the example below.

This configuration mirrors HBase 1.x (primary database) to a Bigtable instance.
```xml
<configuration>
  <property>
    <name>hbase.client.connection.impl</name>
    <value>com.google.cloud.bigtable.mirroring.hbase1_x.MirroringConnection</value>
  </property>

  <!-- Use default HBase connection as primary database connection. -->
  <property>
    <name>google.bigtable.mirroring.primary-client.connection.impl</name>
    <value>default</value>
  </property>

  <property>
    <name>hbase.zookeeper.quorum</name>
    <value>zookeeper-url</value>
  </property>

  <property>
    <name>hbase.zookeeper.property.clientPort</name>
    <value>2181</value>
  </property>

  <!-- Use Bigtable connection as secondary database connection. -->
  <property>
    <name>google.bigtable.mirroring.secondary-client.connection.impl</name>
    <value>com.google.cloud.bigtable.hbase1_x.BigtableConnection</value>
  </property>

  <property>
    <name>google.bigtable.project.id</name>
    <value>project-id</value>
  </property>

  <property>
    <name>google.bigtable.instance.id</name>
    <value>instance-id</value>
  </property>
</configuration>
```

For mirroring HBase 2.x to Bigtable the following keys should be used.
```xml
<configuration>
  <property>
    <name>hbase.client.connection.impl</name>
    <value>com.google.cloud.bigtable.mirroring.hbase2_x.MirroringConnection</value>
  </property>

  <property>
    <name>google.bigtable.mirroring.primary-client.connection.impl</name>
    <value>com.google.cloud.bigtable.hbase2_x.BigtableConnection</value>
  </property>

  <property>
    <name>google.bigtable.mirroring.secondary-client.connection.impl</name>
    <value>default</value>
  </property>

  <property>
    <name>google.bigtable.mirroring.primary-client.async.connection.impl</name>
    <value>com.google.cloud.bigtable.hbase2_x.BigtableAsyncConnection</value>
  </property>

  <property>
    <name>google.bigtable.mirroring.secondary-client.async.connection.impl</name>
    <value>default</value>
  </property>
</configuration>
```

### Prefixes
Both connections are constructed with all keys from the configuration file which means that the bigtable-hbase-java client will have access to HBase keys what shouldn't be a problem in the common case. If it happens to cause some difficulties then prefixes can be used to limit the set of configuration keys that will be available to each connection - setting a `google.bigtable.mirroring.primary-client.prefix` key to `foo` will cause the primary connection to receive only those keys starting with `foo.` prefix, without the prefix. (e.g. `foo.hbase.zookeeper.quorum` would be passed to primary connection as `hbase.zookeeper.quorum`). The user should set either none or both prefix keys. Keys that do not begin with one of the prefixes won't be passed to any of the databases.

## Write modes
Three write modes are available with different guarantees and trade-offs.
|                Mode               |                                                                            Description                                                                            |                                    Source of truth                                    |                      Operation latency                     |                                            Secondary mutation failures                                           |                                               Flow control                                              |
|:---------------------------------:|:-----------------------------------------------------------------------------------------------------------------------------------------------------------------:|:-------------------------------------------------------------------------------------:|:----------------------------------------------------------:|:----------------------------------------------------------------------------------------------------------------:|:-------------------------------------------------------------------------------------------------------:|
| Asynchronous sequential (default) | Operations to the secondary DB are started only after their counterparts to the primary DB finish and complete in the background                                  | A mutation is applied to the secondary DB only if was applied to the primary database | The same as primary DB's (except if flow control kicks in) | Operations which succeeded on the primary DB but failed on the secondary are written to the failed mutation log  | If the mutation backlog to the secondary DB grows beyond a configurable limit, all requests are delayed |
| Synchronous sequential            | Operations to the secondary DB are started only after their counterparts to the primary DB finish and only when both finish are the users' operations completed | Same as Asynchronous sequential                                                       | The latency is the sum of primary and secondary databases  | Operations which succeeded on the primary DB but failed on the secondary are yield the combined operation failed | No additional flow control because no operations are performed in the background                        |
| Synchronous Concurrent            | Operations to both databases are started simultaneously (if they can) and only when both finish are the users' operations completed                             | An operation may be applied only to the primary DB, secondary DB, both or none        | The slower of the two databases                            | If either of the mutations to the primary or secondary DB fails, the user's request fails                        | No additional flow control because no operations are performed in the background                        |

Exceptions thrown in synchronous mode are annotated with MirroringOperationException containing detailed information about failed operations. For more details please consult the docs of this class.

Set `google.bigtable.mirroring.synchronous-writes` to `true` to enable synchronous writes (defaults to false).
Set `google.bigtable.mirroring.concurrent-writes` to `true` to enable concurrent writes (defaults to false).
Asynchronous concurrent mode is not supported.

## Read verification
A fraction of reads performed on the primary database is replayed on the secondary database to verify consistency. Fraction of reads verified can be controlled by `google.bigtable.mirroring.read-verification-rate-percent`. Each read operation, be it a single Get, Get with a list, `batch()` of operations that might contain reads, or even `getScanner(Scan)` - `Results` returned by that operation (or scanner) are verified with the secondary or none of them.

Users can provide custom detection and handling of errors by extending the `MismatchDetector` class and setting `google.bigtable.mirroring.mismatch-detector.factory-impl` key to a path of a class on the classpath.

## Flow control
To limit the number of operations that have not yet been mirrored on the secondary database we've introduced a flow control mechanism that throttles user code performing operations on the primary database if the secondary database is slower and is not keeping up.
The same feature is used to limit the amount of memory used by pending secondary operations.
Operations are throttled until some operations on the secondary database complete (successfully or not) and free up enough resources.
Synchronous operations are throttled by blocking user code in blocking operations until the operation is started on the secondary,
Asynchronous operations are throttled by delaying completing futures until the same moment.

## Faillog
Failed writes to secondary database in asynchronous mode are reported to a Faillog, which, by default dumps failed mutations as JSONs to an on-disk log. The users can replay that log on their own.
Log files location can be controlled by setting `google.bigtable.mirroring.write-error-log.appender.prefix-path` key.  `google.bigtable.mirroring.write-error-log.appender.max-buffer-size` and `google.bigtable.mirroring.write-error-log.appender.drop-on-overflow` can be used to alter default appender behavior.

The user can implement custom handling of failed mutations by overriding default `google.bigtable.mirroring.write-error-consumer.factory-impl` and `google.bigtable.mirroring.write-error-log.appender.factory-impl` keys to user-defined classes on classpath.

## OpenCensus
The Mirroring Client exposes OpenCensus metrics and traces. Metrics are prefixed with `cloud.google.com/java/mirroring`.

## Buffered mutator
Mirroring Client's Buffered Mutator works in two modes: sequential and concurrent.
In the sequential mode, the Mirroring Buffered Mutator passes mutations to the underlying Primary Buffered Mutator and stores mutations in an internal buffer. When the size of the buffer exceeds `google.bigtable.mirroring.buffered-mutator.bytes-to-flush` the Primary Buffered Mutator is flushed. After the flush mutations that did not fail are passed to Secondary Buffered Mutator which is flushed immediately afterwards. The flushes happen asynchronously and do not block user code.
In the concurrent mode writes are passed to both mutators at once. As in sequential mode, the mutations are stored in the internal buffer and a flush is performed periodically. Write errors are reported back to the user when the flush is finished as exceptions when the user interacts with the BufferedMutator for the first time after the errors were detected. Reported exceptions are annotated with MirroringOperationException.

Set `google.bigtable.mirroring.concurrent-writes` to `true` to enable concurrent Buffered Mutator mode (defaults to false).


## Configuration options
- `google.bigtable.mirroring.primary-client.connection.impl` - a name of Connection class that should be used to connect to primary database. It is used as hbase.client.connection.impl when creating connection to primary database. Set to `default` to use default HBase connection class. Required.
- `google.bigtable.mirroring.secondary-client.connection.impl` - a name of Connection class that should be used to connect to secondary database.  It is used as hbase.client.connection.impl when creating connection to secondary database. Set to an `default` to use default HBase connection class. Required.
- `google.bigtable.mirroring.primary-client.async.connection.impl` - a name of Connection class that should be used to connect asynchronously to primary database. It is used as hbase.client.async.connection.impl when creating connection to primary database. Set to `default` to use default HBase connection class. Required when using HBase 2.x.
- `google.bigtable.mirroring.secondary-client.async.connection.impl` - a name of Connection class that should be used to connect asynchronously to secondary database. It is used as hbase.client.async.connection.impl when creating connection to secondary database. Set to `default` to use default HBase connection class. Required when using HBase 2.x.
- `google.bigtable.mirroring.primary-client.prefix` - By default all parameters from the Configuration object passed to ConnectionFactory#createConnection are passed to Connection instances. If this key is set, then only parameters that start with the given prefix are passed to the primary connection. Use it if primary and secondary connections' configurations share a key that should have a different value passed to each of the connections, e.g. zookeeper url.  Prefixes should not contain a dot at the end. default: empty.
- `google.bigtable.mirroring.secondary-client.prefix` - If this key is set, then only parameters that start with given prefix are passed to secondary Connection. default: empty.
- `google.bigtable.mirroring.mismatch-detector.factory-impl` - Path to class implementing MismatchDetector.Factory. default: DefaultMismatchDetector.Factory, logs detected mismatches to stdout and reports them as OpenCensus metrics.
- `google.bigtable.mirroring.flow-controller-strategy.factory-impl` - Path to class to be used as FlowControllerStrategy.Factory. default: RequestCountingFlowControlStrategy.Factory. Used to throttle primary database requests in case of slower secondary.
- `google.bigtable.mirroring.flow-controller-strategy.max-outstanding-requests` - Maximal number of outstanding secondary database requests before throttling requests to primary database. default: 500.
- `google.bigtable.mirroring.flow-controller-strategy.max-used-bytes` - Maximal number of bytes used by internal buffers for asynchronous operations before throttling requests to primary database. default: 256MB.
- `google.bigtable.mirroring.write-error-consumer.factory-impl` - Path to a factory of a class to be used as consumer for secondary database write errors. default: DefaultSecondaryWriteErrorConsumer.Factory, forwards errors to faillog using Appender and Serializer.
- `google.bigtable.mirroring.write-error-log.serializer.factory-impl` - Factory of faillog Serializer class implementation, responsible for serializing write errors reported by the Logger to binary representation, which is later appended to resulting file by the Appender. default: DefaultSerializer.Factory, dumps supplied mutation along with error stacktrace as JSON.
- `google.bigtable.mirroring.write-error-log.appender.factory-impl` - Factory of faillog Appender class implementation. default: DefaultAppender.Factory, writes data serialized by Serializer implementation to file on disk.
- `google.bigtable.mirroring.write-error-log.appender.prefix-path` - used by DefaultAppender, prefix used for generating the name of the log file. Required.
- `google.bigtable.mirroring.write-error-log.appender.max-buffer-size` - used by DefaultAppender, maxBufferSize maximum size of the buffer used for communicating with the thread flushing the data to disk. default: 20971520 bytes (20 MB).
- `google.bigtable.mirroring.write-error-log.appender.drop-on-overflow` - used by DefaultAppender, whether to drop data if the thread flushing the data to disk is not keeping up or to block until it catches up. default: false.
- `google.bigtable.mirroring.read-verification-rate-percent` - Integer value representing percentage of read operations performed on primary database that should be verified against secondary. Each call to `Table#get(Get)`, `Table#get(List)`, `Table#exists(Get)`, `Table#existsAll(List)`, `Table#batch(List, Object[])` (with overloads) and `Table#getScanner(Scan)` (with overloads) is counted as a single operation, independent of size of their arguments and results. Correct values are a integers ranging from 0 to 100 inclusive. default: 100.
- `google.bigtable.mirroring.buffered-mutator.bytes-to-flush` - Number of bytes that `MirroringBufferedMutator` should buffer before flushing underlying primary BufferedMutator and executing a write to the secondary database. If not set the value of `hbase.client.write.buffer` is used, which by default is 2MB. When those values are kept in sync, the mirroring client should perform a flush operation on the primary BufferedMutator right after it schedules a new asynchronous write to the database.


## Caveats
### Timestamps
For ensuring full consistency between databases the user should always specify a timestamp for mutations issued using MirroringClient. Mutations without a timestamp will have one assigned by underlying database clients when the mutations are issued to underlying databases, what doesn't happen at the exact same instant for both databases.
### Differences between Bigtable and HBase
There are differences between HBase and Bigtable, please consult [this link](https://cloud.google.com/bigtable/docs/hbase-differences).
Code using this client should be aware of them.
### Mirroring Increments and Appends
`increment` and `append` operations do not allow to specify a timestamp of the new version to create. To keep databases consistent the Mirroring Client mirrors these operations as `Put`s inserting return values of these methods. This also applies to `Increment`s and `Append`s performed in `batch()` operation. For that reason, those operations have to be mirrored sequentially, even if concurrent write mode is enabled.
### Verification of 2.x scans is not performed
`AsyncTable#scan(Scan)` operation results are not verified for consistency with the secondary database. bigtable-hbase-java client doesn't support AdvancedScanResultConsumer and we would not be able to throttle its operations when Bigtable would be used as a primary database and the secondary database would be significantly slower.
