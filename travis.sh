#!/usr/bin/env bash

set -e  # exit immediately on error
set -x  # display all commands

MVN="./mvnw --batch-mode"

# build & run tests
$MVN clean verify \
  | egrep -v "(^\[INFO\] Download|^\[INFO\].*skipping)"

# TODO(igorbernstein2): enable integration tests
