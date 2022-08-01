# Replication client log storage design

This doc summarizes high-level design decisions on how to store failed
mutations.

## Use cases

* reapplying failed mutations
* debugging information
* monitoring purposes

## Requirements

* The storage must survive machine crashes
* Writing to that file must not block the process in the common case
* There has to be an upper limit on how much RAM it uses
* The log has to be machine readable
* Log entries may grow up to 10MB
* Configurability - the user may wish to e.g. stream the logs to a distributed
  queue, central logging solution or something else
* Performance should not be a problem
* Good fit into existing centralized log collection solutions

## Design decisions

* For every failed mutation, we log a structure containing:
  * timestamp
  * error details
  * failed mutation
  * operation specific context (i.e. condition of the conditional mutation or
    index in the bulk mutation or read-modify-write proto)
* We log JSON - one message per one line; that way all central logging solutions
  will be able to digest such data in a meaningful way
* We write our own, custom logging mechanism
* We leave it to the user to decide what happens in case writing the logs is
  becoming the bottleneck (i.e. drop the logs or slow down the client)

## Sketch of the design

Following log4j's approach, we'll implement 3 components:
* `Appender`, whose responsibility will be to asynchronously write the
  serialized mutations to files
* `Serializer`, whose responsibility is to pack the failed mutations and their
  contexts into log entries, which in our case will be single-line JSONs
* `Logger`, whose responsibility it is to expose an API for logging failed
  mutations and forward them to the `Appender` through the `Serializer`

The user will be able to swap the `Serializer` and `Appender` according to their
liking.

The `Appender` will simply spin up a thread flushing the data to disk and use a
bounded buffer as the entry point for log entries. The user will be able to
choose whether it should block or drop excessive mutations in case flushing the
log is not keeping up.

## Motivation

* we fit into the existing log gathering ecosystem, allowing for use of standard
  tools for log shipping, analysis and collection
* we do not require dependencies on libraries which could potentially conflict
  with the ones which the user chooses (e.g. log4j)

## Alternatives considered

### log4j + Jackson

It is tempting to use existing logging infrastructure to do the job. E.g. if we
used log4j and Jackson, we could utilize the standard Java's logging
infrastructure with its plentiful configuration options and achieve the same
goal with much less code.

This approach is not feasible, though, because it would mean including log4j in
the dependencies, potentially clashing with the logging library of users'
choice. Even if we used shading, hard to debug problems are likely because
log4j uses the global environment (log4j.properties, or Java's system
properties).

### protobuf instead of JSON

Instead of using JSON, protobuf could be used for better space/CPU efficiency.
While this is tempting, JSON has the advantage of being human-readable and
adopted by most centralized logging systems. This seems to outweigh protobuf's
benefits. If the user wants protobuf, they can still swap the relevant part
to achieve that.
