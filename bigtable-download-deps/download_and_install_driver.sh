#!/bin/bash
# download_and_install_driver.sh
# Download a version of driver_with_deps.jar and install it in maven

REMOTE_DIRECTORY=$1
FILE=$2
TMP_DIR=$3
GROUP=$4
ARTIFACT_ID=$5
VERSION=$6
REPO_PATH=$7

mkdir -p ${TMP_DIR}
echo "mkdir -p ${TMP_DIR}"

echo "gsutil cp ${REMOTE_DIRECTORY}/${FILE} ${TMP_DIR}"
gsutil cp ${REMOTE_DIRECTORY}/${FILE} ${TMP_DIR}

echo "mvn org.apache.maven.plugins:maven-install-plugin:2.3.1:install-file \
   -Dfile=${TMP_DIR}/${FILE} \
   -DgroupId=${GROUP} \
   -DartifactId=${ARTIFACT_ID} \
   -Dversion=${VERSION} \
   -Dpackaging=jar \
   -DlocalRepositoryPath=${REPO_PATH}"

mvn org.apache.maven.plugins:maven-install-plugin:2.3.1:install-file \
   -Dfile=${TMP_DIR}/${FILE} \
   -DgroupId=${GROUP} \
   -DartifactId=${ARTIFACT_ID} \
   -Dversion=${VERSION} \
   -Dpackaging=jar \
   -DlocalRepositoryPath=${REPO_PATH}

