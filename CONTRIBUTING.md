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
2. There is also a suite of integration tests that connect to a cluster you have access to, via the authentication credentials that were loaded during the Google Cloud SDK configuration step (see above).

   Use the following command to run the Cloud Bigtable integration tests for HBase 1:

   ```sh
   mvn clean verify \
       -PbigtableIntegrationTest \
       -Dgoogle.bigtable.project.id=[your cloud project id] \
       -Dgoogle.bigtable.instance.id=[your cloud bigtable instance id]
   ```

   Use the following command to run the Cloud Bigtable integration tests for HBase 2:

   ```sh
   mvn clean verify \
       -PbigtableIntegrationTestH2 \
       -Dgoogle.bigtable.project.id=[your cloud project id] \
       -Dgoogle.bigtable.instance.id=[your cloud bigtable instance id]
   ```
   
   There are also tests that perform compatibility tests against an HBase Minicluster, which can be invoked with the following commands for HBase 1 and HBase 2 respectively: 
   ```sh
   mvn clean verify -PhbaseLocalMiniClusterTest
   ```
   ```sh
   mvn clean verify -PhbaseLocalMiniClusterTestH2
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

## Building the project

To build, package, and run all unit tests run the command

```
mvn clean verify
```

### Running Integration tests

To include integration tests when building the project, you need access to
a GCP Project with a valid service account. 

For instructions on how to generate a service account and corresponding
credentials JSON see: [Creating a Service Account][1].

Then run the following to build, package, run all unit tests and run all
integration tests.

```bash
export GOOGLE_APPLICATION_CREDENTIALS=/path/to/service/account.json
mvn -Penable-integration-tests clean verify
```

## Code Samples

Code Samples must be bundled in separate Maven modules, and guarded by a
Maven profile with the name `enable-samples`.

The samples must be separate from the primary project for a few reasons:
1. Primary projects have a minimum Java version of Java 7 whereas samples have
   a minimum Java version of Java 8. Due to this we need the ability to
   selectively exclude samples from a build run.
2. Many code samples depend on external GCP services and need
   credentials to access the service.
3. Code samples are not released as Maven artifacts and must be excluded from 
   release builds.
   
### Building

```bash
mvn -Penable-samples clean verify
```

Some samples require access to GCP services and require a service account:

```bash
export GOOGLE_APPLICATION_CREDENTIALS=/path/to/service/account.json
mvn -Penable-samples clean verify
```

### Profile Config

1. To add samples in a profile to your Maven project, add the following to your
`pom.xml`

    ```xml
    <project>
      [...]
      <profiles>
        <profile>
          <id>enable-samples</id>
          <modules>
            <module>sample</module>
          </modules>
        </profile>
      </profiles>
      [...]
    </project>
    ```

2. [Activate](#profile-activation) the profile.
3. Define your samples in a normal Maven project in the `samples/` directory.

### Code Formatting

Code in this repo is formatted with
[google-java-format](https://github.com/google/google-java-format).
To run formatting on your project, you can run:
```
mvn com.coveo:fmt-maven-plugin:format
```

### Profile Activation

To include code samples when building and testing the project, enable the 
`enable-samples` Maven profile.

#### Command line

To activate the Maven profile on the command line add `-Penable-samples` to your
Maven command.

#### Maven `settings.xml`

To activate the Maven profile in your `~/.m2/settings.xml` add an entry of
`enable-samples` following the instructions in [Active Profiles][2].

This method has the benefit of applying to all projects you build (and is
respected by IntelliJ IDEA) and is recommended if you are going to be
contributing samples to several projects.

#### IntelliJ IDEA

To activate the Maven Profile inside IntelliJ IDEA, follow the instructions in
[Activate Maven profiles][3] to activate `enable-samples`.

[1]: https://cloud.google.com/docs/authentication/getting-started#creating_a_service_account
[2]: https://maven.apache.org/settings.html#Active_Profiles
[3]: https://www.jetbrains.com/help/idea/work-with-maven-profiles.html#activate_maven_profiles
