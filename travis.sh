#!/usr/bin/env bash

set -e  # exit immediately on error
set -x  # display all commands

export MAVEN_OPTS="-Xmx1024m"

MVN="mvn --batch-mode"

# build & run tests
$MVN clean verify \
  | egrep -v "(^\[INFO\] Download|^\[INFO\].*skipping)"

# TODO(igorbernstein2): enable integration tests