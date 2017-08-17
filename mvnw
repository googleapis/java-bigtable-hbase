#!/usr/bin/env bash

# Travis will use this wrapper script instead of the regular mvn command when installing deps
# before running the script command.

# print some useful stats about the build run after it completes
/usr/bin/time -v mvn "$@"
