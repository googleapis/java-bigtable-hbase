#!/bin/bash

GRADLE_USER_HOME="${PWD}"
M2_HOME="${GRADLE_USER_HOME}/.m2"
MAVEN_SETTINGS="${M2_HOME}/settings.xml"
MAVEN_REPOSITORY="${PWD}/.repository/"
mkdir -p "${M2_HOME}"
mkdir -p "${MAVEN_REPOSITORY}"

cat >$MAVEN_SETTINGS <<EOF
<settings>
  <localRepository>${MAVEN_REPOSITORY}</localRepository>
</settings>
EOF


pushd lib/grpc-java/lib/netty
mvn install -pl codec-http2 -am -DskipTests=true -Dmaven.repo.local="${MAVEN_REPOSITORY}"
popd

pushd lib/grpc-java
./gradlew -Duser.home="${GRADLE_USER_HOME}" install
popd
