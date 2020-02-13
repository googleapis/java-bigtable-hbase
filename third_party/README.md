# Third Party Directory
This directory contains third party code which has been slightly modified for Bigtable specific use cases. 
Classes are added as additional source files to their corresponding modules. Example from bigtable-beam-import's pom.xml below:
  ```xml
<plugin>
    <groupId>org.codehaus.mojo</groupId>
    <artifactId>build-helper-maven-plugin</artifactId>
    <version>3.0.0</version>
    <executions>
        <execution>
         <id>add-source</id>
         <phase>generate-sources</phase>
         <goals>
           <goal>add-source</goal>
         </goals>
         <configuration>
           <sources>
             <source>../../third_party/third_party_hbase_server/src/main/import/</source>
           </sources>
         </configuration>
        </execution>
    </executions>
</plugin>
  ```