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
  <properties>
    <bigtable.emulator.properties.path>${project.build.testOutputDirectory}/bigtable-emulator.properties</bigtable.emulator.properties.path>
  </properties>
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
                      <propertiesPath>${bigtable.emulator.properties.path}</propertiesPath>
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
                    <systemPropertiesFile>${bigtable.emulator.properties.path}</systemPropertiesFile>
                    <includes>**/*IT.java</includes>
                </configuration>
            </execution>
        </executions>
    </plugin>
  </build>
  ```
- In your integration test, connect to the emulator:
  ```java
      BigtableOptions opts = new BigtableOptions.Builder()
          .setUserAgent("fake")
          .setCredentialOptions(CredentialOptions.nullCredential())
          .setUsePlaintextNegotiation(true)
          .setPort(Integer.parseInt(System.getProperty("google.bigtable.endpoint.port")))
          .setDataHost(System.getProperty("google.bigtable.endpoint.host"))
          .setInstanceAdminHost(System.getProperty("google.bigtable.instance.admin.endpoint.host"))
          .setTableAdminHost(System.getProperty("google.bigtable.admin.endpoint.host"))
          .setProjectId(System.getProperty("google.bigtable.project.id"))
          .setInstanceId(System.getProperty("google.bigtable.instance.id"))
          .build();
  ```
  Or, for hbase:
  ```java
      Configuration config = HBaseConfiguration.create();
      Properties properties = new Properties();

      try (InputStream in = Moo.class.getResourceAsStream("bigtable-emulator.properties")) {
        properties.load(in);
      }

      for (Entry<Object, Object> entry : properties.entrySet()) {
        config.set((String)entry.getKey(), (String)entry.getValue());
      }
      Connection connection = BigtableConfiguration.connect(config);
   ```
