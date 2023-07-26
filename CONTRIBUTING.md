# How to Contribute

We'd love to accept your patches and contributions to this project. There are
just a few small guidelines you need to follow.

## Contributor License Agreement

Contributions to this project must be accompanied by a Contributor License
Agreement. You (or your employer) retain the copyright to your contribution;
this simply gives us permission to use and redistribute your contributions as
part of the project. Head over to <https://cla.developers.google.com/> to see
your current agreements on file or to sign a new one.

You generally only need to submit a CLA once, so if you've already submitted one
(even if it was for a different project), you probably don't need to do it
again.

## Developing and testing

1. Run `mvn clean install` to build and install Cloud Bigtable client artifacts to your local repository, and then run the unit tests.
2. There is also a suite of integration tests that connect to a cluster you have access to, via the authentication credentials that were loaded during the Google Cloud SDK configuration step (see [here](https://github.com/googleapis/google-cloud-java#authentication)).

   Use the following command to run the Cloud Bigtable integration tests for HBase 1:

   ```sh
   mvn clean verify \
       -Penable-integration-tests \
       -PbigtableIntegrationTest \
       -Dgoogle.bigtable.project.id=[your cloud project id] \
       -Dgoogle.bigtable.instance.id=[your cloud bigtable instance id]
   ```

   Use the following command to run the Cloud Bigtable integration tests for HBase 2:

   ```sh
   mvn clean verify \
       -Penable-integration-tests \
       -PbigtableIntegrationTestH2 \
       -Dgoogle.bigtable.project.id=[your cloud project id] \
       -Dgoogle.bigtable.instance.id=[your cloud bigtable instance id]
   ```
   
   There are also tests that perform compatibility tests against an HBase Minicluster, which can be invoked with the following commands for HBase 1 and HBase 2 respectively: 
   ```sh
   mvn clean verify -Penable-integration-tests -PhbaseLocalMiniClusterTest
   ```
   ```sh
   mvn clean verify -Penable-integration-tests -PhbaseLocalMiniClusterTestH2
   ```

   You can run those commands at the top of the project, or you can run them at the appropriate integration-tests project.  
   
   Developer's NOTE: You can build the project faster by running the following command, and then run the integration test command from the appropriate integration test directory:
   
   ```sh
   mvn -pl bigtable-hbase-1.x-parent/bigtable-hbase-1.x-integration-tests \
   -pl bigtable-hbase-2.x-parent/bigtable-hbase-2.x-integration-tests \
   -am clean install
   ```
3. Run `mvn -X com.coveo:fmt-maven-plugin:format` to check and enforce consistent java formatting to all of your code changes.

4. You can use [google-cloud-bigtable-emulator][google-cloud-bigtable-emulator] for local testing. The minimum configuration requires to connect the emulator with `bigtable-core-client` OR `hbase-client`:
    ```java
    @Rule
    public final BigtableEmulatorRule bigtableEmulator = BigtableEmulatorRule.create();
    private BigtableSession session;
    private Connection connection;
    
    @Setup
    public void setup() {
      // for bigtable-core
      BigtableOptions opts = BigtableOptions.builder()
            .enableEmulator("localhost:" + bigtableEmulator.getPort())
            .setUserAgent("fake")
            .setProjectId("fakeproject")
            .setInstanceId("fakeinstance")
            .build();
      this.session = new BigtableSession(opts);
    
      // for hbase
      Configuration config = BigtableConfiguration.configure("fakeproject", "fakeinstance");
      config.set("google.bigtable.emulator.endpoint.host", "localhost:" + bigtableEmulator.getPort());
      this.connection = BigtableConfiguration.connect(config);
    }
    ```

NOTE: This project uses extensive shading which IDEs have trouble with. To overcome these issues,
disable the `with-shaded` profile in your IDE to force it to resolve the dependencies from your local
maven repository. When you disable that profile, attach workspace sources to the local maven repository jars.

## Code reviews

All submissions, including submissions by project members, require review. We
use GitHub pull requests for this purpose. Consult
[GitHub Help](https://help.github.com/articles/about-pull-requests/) for more
information on using pull requests.

## Community Guidelines

This project follows
[Google's Open Source Community Guidelines](https://opensource.google.com/conduct/).

### Code Formatting

Code in this repo is formatted with
[google-java-format](https://github.com/google/google-java-format).
To run formatting on your project, you can run:
```
mvn com.coveo:fmt-maven-plugin:format
```

[1]: https://cloud.google.com/docs/authentication/getting-started#creating_a_service_account
[2]: https://maven.apache.org/settings.html#Active_Profiles
[3]: https://www.jetbrains.com/help/idea/work-with-maven-profiles.html#activate_maven_profiles
