# bigtable-build-helper integration tests

This directory contains test projects that will be invoked to test all of the
custom maven goals. Each directory represents a testcase. A testcase contains
a pom.xml that will be built. By default, if the test pom.xml fails, the test
is marked as failed. However, `invoker.properties` is used to modify the
behavior. If `invoker.buildResult = failure` is set, it will invert the behvior.
`verify.bsh` is a verification script that will be used to check additional
conditions after the build completes. The output of each build is written to
`target/it/test-name/build.log`. See https://maven.apache.org/plugins/maven-invoker-plugin/usage.html
for more details.
