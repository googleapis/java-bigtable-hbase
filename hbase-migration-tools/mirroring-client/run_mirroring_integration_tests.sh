#!/bin/bash
set -euo pipefail

scriptDir=$(realpath $(dirname "${BASH_SOURCE[0]}"))

# cd to root of git repo
cd ${scriptDir}/../..

targets=(
  hbase-migration-tools/mirroring-client/bigtable-hbase-mirroring-client-1.x-parent/bigtable-hbase-mirroring-client-1.x-integration-tests
  hbase-migration-tools/mirroring-client/bigtable-hbase-mirroring-client-2.x-parent/bigtable-hbase-mirroring-client-2.x-integration-tests
  hbase-migration-tools/mirroring-client/bigtable-hbase-mirroring-client-2.x-parent/bigtable-hbase-mirroring-client-1.x-2.x-integration-tests
)

# install all of the dependencies into the local repository
echo "Installing all artifacts into local maven repository"
mvn clean install -DskipTests -am -pl $( IFS=, ; echo "${targets[*]}" ),bigtable-test/bigtable-emulator-maven-plugin,bigtable-test/bigtable-build-helper

# Then run integration tests on the leaves
cmd="mvn verify -pl $( IFS=, ; echo "${targets[*]}" ) -Penable-integration-tests,"

hbaseToBigtableTargets=HBaseToBigtableLocalIntegrationTests,HBase12ToBigtableLocalIntegrationTests,HBase2ToBigtableLocalIntegrationTests
bigtableToHbaseTargets=BigtableToHBaseLocalIntegrationTests,BigtableToHBase12LocalIntegrationTests,BigtableToHBase2LocalIntegrationTests

# We have to run mvn twice - once to test HBase to Bigtable mirroring and once again to test Bigtable to HBase mirroring,
# because they run the same code but with distinct configurations.
# We are not able to test every case with just HBase -> Bigtable mirroring because we emulate server-side errors by injecting
# custom HRegion implementation into HBase MiniCluster. Bigtable -> HBase mirroring run is required to verify if errors from
# secondary database are handled correctly.
echo "Running hbaseToBigtableTargets"
${cmd}${hbaseToBigtableTargets}
echo "Running bigtableToHbaseTargets"
${cmd}${bigtableToHbaseTargets}
