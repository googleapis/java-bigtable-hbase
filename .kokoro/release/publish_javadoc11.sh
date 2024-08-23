#!/bin/bash
# Copyright 2021 Google LLC
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

set -eo pipefail

if [[ -z "${CREDENTIALS}" ]]; then
  CREDENTIALS=${KOKORO_KEYSTORE_DIR}/73713_docuploader_service_account
fi

if [[ -z "${STAGING_BUCKET_V2}" ]]; then
  echo "Need to set STAGING_BUCKET_V2 environment variable"
  exit 1
fi

# work from the git root directory
pushd $(dirname "$0")/../../

# install docuploader package
python3 -m pip install --require-hashes -r .kokoro/requirements.txt

# compile all packages
mvn clean install -B -q -DskipTests=true

export NAME=bigtable-client-parent
export VERSION=$(grep ${NAME}: versions.txt | cut -d: -f3)

# cloud RAD generation
#TODO(https://github.com/googleapis/java-bigtable-hbase/issues/3843) - mvn compile breaks for the project, hence using the workaround below
# use javadoc:aggregate for the following modules
cd bigtable-client-core-parent/
mvn clean javadoc:aggregate -B -q -P docFX
cd ../bigtable-dataflow-parent/
mvn clean javadoc:aggregate -B -q -P docFX
cd ../bigtable-hbase-2.x-parent/
mvn clean javadoc:aggregate -B -q -P docFX

# use javadoc:javadoc for the following modules for Standalone doc generation as javadoc:aggregate breaks due to compilation issues
cd ../hbase-migration-tools/
mvn clean javadoc:javadoc
cd ../bigtable-hbase-1.x-parent/
mvn clean javadoc:javadoc

# go back to the project root directory
cd ..

# include CHANGELOG
cp CHANGELOG.md target/docfx-yml/history.md

pushd target/docfx-yml

# create metadata
python3 -m docuploader create-metadata \
 --name ${NAME} \
 --version ${VERSION} \
 --xrefs devsite://java/gax \
 --xrefs devsite://java/google-cloud-core \
 --xrefs devsite://java/api-common \
 --xrefs devsite://java/proto-google-common-protos \
 --xrefs devsite://java/google-api-client \
 --xrefs devsite://java/google-http-client \
 --xrefs devsite://java/protobuf \
 --language java

# upload yml to production bucket
python3 -m docuploader upload . \
 --credentials ${CREDENTIALS} \
 --staging-bucket ${STAGING_BUCKET_V2} \
 --destination-prefix docfx
