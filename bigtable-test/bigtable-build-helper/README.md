# Cloud Bigtable build helpers

This module is used exclusively by the java-bigtable-hbase build process.
It contains various maven plugins to help maintain this project.
Specifically it contains the following maven goals:

- verify-shaded-jar-entries: this ensures that the shaded jars don't accidentally
  leak classes outside java-bigtable-hbase's namespace. It takes a list of
  allowed roots and ensures that the shaded jar doesn't include classes outside
  of those roots.
- verify-shaded-exclusions: this ensures that if a dependency is excluded from
  the shaded jar, that the resulting pom.xml will include it as an external
  dependency.
- verify-mirror-deps : this ensures that the current module only contains a
  proper superset of depedencies of the specified artifact. In other words, it
  ensures that artifacts like bigtable-hbase-1.x-hadoop doesnt introduce
  dependency conflicts with hbase-client.

## Publishing

This module is explicitly not published to maven central since it is only used
during build.

 ## Testing
 
Integration tests are bit nonstandard as they test maven itself. This module
uses `maven-invoker-plugin` to test each goal. All of the tests live in the
`src/it` directory. Each test consists of `pom.xml` that defines a test scenario,
and an optional verification script. If the verification script is absent, the
build will only be tested for a successful run. If a verification script is 
present, it tests the output of the build. Furthermore the test verification
behavior can be modified via `invoker.properties` file. Each test writes the 
output of each maven invocation under `target/it/{test-name}/build.log`.

Information is available here: https://maven.apache.org/plugins/maven-invoker-plugin/.
