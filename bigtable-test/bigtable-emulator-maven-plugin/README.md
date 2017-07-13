# Cloud Bigtable emulator maven plugin

WARNING: This is still experimental.

This project provides maven integration for the [cloud bigtable emulator](https://cloud.google.com/bigtable/docs/emulator).
It provides 3 maven goals:

* start: starts the emulator and writes a bigtable-emulator.properties to your test classpath with the port of the emulator
* stop: stops the emulator
* run: like start, but runs the emulator in foreground


Usage:

- Install the [gcloud sdk](https://cloud.google.com/sdk/downloads)
- Install the bigtable emulator:
  ```gcloud components install cbt```
- Add the plugin to your pom.xml and configure the maven failsafe plugin:
  ```xml
  <build>
      <plugin>
          <groupId>com.google.cloud.bigtable</groupId>
          <artifactId>bigtable-emulator-maven-plugin</artifactId>
          <version>1.0.0-pre2</version>
          <executions>
              <execution>
                  <goals>
                      <goal>start</goal>
                      <goal>stop</goal>
                  </goals>
                  <configuration>
                      <propertyName>bigtable.emulator.endpoint</propertyName>
                  </configuration>
              </execution>
          </executions>
    </plugin>
    <plugin>
        <artifactId>maven-failsafe-plugin</artifactId>
        <version>2.19.1</version>
        <executions>
            <execution>
                <goals>
                    <goal>integration-test</goal>
                    <goal>verify</goal>
                </goals>
                <configuration>
                    <environmentVariables>
                        <BIGTABLE_EMULATOR_HOST>${bigtable.emulator.endpoint}</BIGTABLE_EMULATOR_HOST>
                    </environmentVariables>
                </configuration>
            </execution>
        </executions>
    </plugin>
  </build>
  ```
- Your integration tests will read the `BIGTABLE_EMULATOR_HOST` environment variable and connect to the emulator:
  ```java
      BigtableOptions opts = new BigtableOptions.Builder()
          .setUserAgent("fake")
          .setProjectId("fakeproject")
          .setInstanceId("fakeinstance")
          .build();
  ```
  Or, for hbase:
  ```java
      Connection connection = BigtableConfiguration.connect("fakeproject", "fakeinstace");
   ```
