# Copyright 2020 Google LLC
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""This script is used to synthesize generated parts of this library."""

import synthtool.languages.java as java

java.common_templates(excludes=[
  'README.md',
  'CONTRIBUTING.md',
  '.github/ISSUE_TEMPLATE/bug_report.md',
  '.github/snippet-bot.yml',
  '.github/release-please.yml',
  '.github/CODEOWNERS',
  '.github/workflows/*',
  '.kokoro/linkage-monitor.sh',
  '.kokoro/presubmit/linkage-monitor.cfg',
  '.kokoro/presubmit/integration.cfg',
  '.kokoro/presubmit/graalvm-native.cfg',
  '.kokoro/presubmit/graalvm-native-17.cfg',
  '.kokoro/nightly/integration.cfg',
  '.kokoro/nightly/java11-integration.cfg',
  '.kokoro/presubmit/java7.cfg',
  '.kokoro/continuous/java7.cfg',
  '.kokoro/nightly/java7.cfg',
  '.kokoro/dependencies.sh',
  '.kokoro/build.sh',
  '.kokoro/build.bat',
  'samples/*',
  'renovate.json'
])
