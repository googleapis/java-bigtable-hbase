#!/bin/bash

DIRECTORY=$1
FILE=$2
GROUP=$3
ARTIFACT_ID=$4
VERSION=$5
REPO_PATH=$6

echo "cd ../bigtable-grpc-interface"
cd ../bigtable-grpc-interface

echo "mvn package"
mvn package

BT_GRPC_JAR=target/$FILE

echo "mvn org.apache.maven.plugins:maven-install-plugin:2.3.1:install-file \
   -Dfile=${BT_GRPC_JAR} \
   -DgroupId=${GROUP} \
   -DartifactId=${ARTIFACT_ID} \
   -Dversion=${VERSION} \
   -Dpackaging=jar \
   -DlocalRepositoryPath=${REPO_PATH}"

mvn org.apache.maven.plugins:maven-install-plugin:2.3.1:install-file \
   -Dfile=${BT_GRPC_JAR} \
   -DgroupId=${GROUP} \
   -DartifactId=${ARTIFACT_ID} \
   -Dversion=${VERSION} \
   -Dpackaging=jar \
   -DlocalRepositoryPath=${REPO_PATH}
