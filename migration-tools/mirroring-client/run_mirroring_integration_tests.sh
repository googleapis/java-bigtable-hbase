#!/bin/bash
set -euo pipefail

scriptDir=$(realpath $(dirname "${BASH_SOURCE[0]}"))

# cd to root of git repo
cd ${scriptDir}/../..

targets=(
  migration-tools/mirroring-client/bigtable-hbase-mirroring-client-core-parent/protobuf-java-format-shaded
  migration-tools/mirroring-client/bigtable-hbase-mirroring-client-core-parent/bigtable-hbase-mirroring-client-core
  migration-tools/mirroring-client/bigtable-hbase-mirroring-client-1.x-parent/bigtable-hbase-mirroring-client-1.x
  migration-tools/mirroring-client/bigtable-hbase-mirroring-client-1.x-parent/bigtable-hbase-mirroring-client-1.x-integration-tests
  migration-tools/mirroring-client/bigtable-hbase-mirroring-client-2.x-parent/bigtable-hbase-mirroring-client-2.x
  migration-tools/mirroring-client/bigtable-hbase-mirroring-client-2.x-parent/bigtable-hbase-mirroring-client-2.x-integration-tests
  migration-tools/mirroring-client/bigtable-hbase-mirroring-client-2.x-parent/bigtable-hbase-mirroring-client-1.x-2.x-integration-tests
)

cmd="mvn clean compile verify -pl $( IFS=, ; echo "${targets[*]}" ) -Penable-integration-tests,"

# TODO: fix HBase 2.x ITs.
hbaseToBigtableTargets=HBaseToBigtableLocalIntegrationTests #,HBase12ToBigtableLocalIntegrationTests,HBase2ToBigtableLocalIntegrationTests
bigtableToHbaseTargets=BigtableToHBaseLocalIntegrationTests #,BigtableToHBase12LocalIntegrationTests,BigtableToHBase2LocalIntegrationTests

# We have to run mvn twice - once to test HBase to Bigtable mirroring and once again to test Bigtable to HBase mirroring,
# because they run the same code but with distinct configurations.
${cmd}${hbaseToBigtableTargets}
${cmd}${bigtableToHbaseTargets}
