#!/usr/bin/env bash

set -e  # exit immediately on error
set -x  # display all commands

# Limit the amount of memory maven can use to avoid hitting the 3GB build limit in travis
export MAVEN_OPTS="-Xmx1024m"

# build & run tests
/usr/bin/time -v \
  mvn --batch-mode clean verify \
  | egrep -v "(^\[INFO\] Download|^\[INFO\].*skipping)"

# TODO(igorbernstein2): enable integration tests
