#!/bin/bash
set -euo pipefail

scriptDir=$(realpath $(dirname "${BASH_SOURCE[0]}"))

# cd to root of git repo
cd ${scriptDir}/../..

targets=(
  hbase-migration-tools/mirroring-client/bigtable-hbase-mirroring-client-core-parent/protobuf-java-format-shaded
  hbase-migration-tools/mirroring-client/bigtable-hbase-mirroring-client-core-parent/bigtable-hbase-mirroring-client-core
  hbase-migration-tools/mirroring-client/bigtable-hbase-mirroring-client-1.x-parent/bigtable-hbase-mirroring-client-1.x
  hbase-migration-tools/mirroring-client/bigtable-hbase-mirroring-client-1.x-parent/bigtable-hbase-mirroring-client-1.x-integration-tests
  hbase-migration-tools/mirroring-client/bigtable-hbase-mirroring-client-2.x-parent/bigtable-hbase-mirroring-client-2.x
  hbase-migration-tools/mirroring-client/bigtable-hbase-mirroring-client-2.x-parent/bigtable-hbase-mirroring-client-2.x-integration-tests
  hbase-migration-tools/mirroring-client/bigtable-hbase-mirroring-client-2.x-parent/bigtable-hbase-mirroring-client-1.x-2.x-integration-tests
)

cmd="mvn clean compile verify -pl $( IFS=, ; echo "${targets[*]}" ) -Penable-integration-tests,"

hbaseToBigtableTargets=HBaseToBigtableLocalIntegrationTests,HBase12ToBigtableLocalIntegrationTests,HBase2ToBigtableLocalIntegrationTests
bigtableToHbaseTargets=BigtableToHBaseLocalIntegrationTests,BigtableToHBase12LocalIntegrationTests,BigtableToHBase2LocalIntegrationTests

# We have to run mvn twice - once to test HBase to Bigtable mirroring and once again to test Bigtable to HBase mirroring,
# because they run the same code but with distinct configurations.
# We are not able to test every case with just HBase -> Bigtable mirroring because we emulate server-side errors by injecting
# custom HRegion implementation into HBase MiniCluster. Bigtable -> HBase mirroring run is required to verify if errors from
# secondary database are handled correctly.
${cmd}${hbaseToBigtableTargets}
${cmd}${bigtableToHbaseTargets}
