#!/usr/bin/env bash

set -e  # exit immediately on error
set -x  # display all commands

MVN="mvn --batch-mode"

# build & run tests
$MVN clean install \
  | egrep -v "(^\[INFO\] Download|^\[INFO\].*skipping)"

# run integration tests using a specific hbase version
$MVN verify \
  -pl bigtable-hbase-parent/bigtable-hbase-integration-tests \
  -PhbaseLocalMiniClusterTest \
  -Dhbase.version=${INT_HBASE:-1.3.0} \
  -Dgoogle.bigtable.project.under.test=${INT_TARGET:-bigtable-hbase-1.3} \
  -Dgoogle.bigtable.connection.impl=${INT_CONN:-com.google.cloud.bigtable.hbase1_3.BigtableConnection} \
  | egrep -v "(^\[INFO\] Download|^\[INFO\].*skipping)"
