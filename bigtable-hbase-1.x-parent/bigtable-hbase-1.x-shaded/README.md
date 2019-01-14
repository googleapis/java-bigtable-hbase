This artifact is used in situations where dependencies don't converge properly.  Hadoop is one of
those environments.  Hadoop uses really old versions of netty, protobuf and guava.  The simplest
way to deal with that problem is to have a shaded jar.

** NOTE TO DEVELOPERS:
Shading can complicate dependency upgrades.  If you upgrade something, and your testing find that
there's a `ClassNotFound`, or something similar, you will have update the pom.xml
of this project in the `org.apache.maven.plugins:maven-shade-plugin` section:

1. You'll have to add an `<include>` for your dependency.  One way to  find the right package is
by runing `mvn clean package` on this project, and looking through the excluded libraries.

2. Add a `<relocation>` for the new inclusion

3. Confirm that the new inclusion is indeed relocated, and that no other classes from other have
been added to the shaded jar.  You can do that by running the following command:

> jar -tf target/bigtable-hbase-*-shaded-*-SNAPSHOT.jar | grep class | grep -v repackaged |  grep -v hbase

That should only return classes that start with `com/google/cloud/bigtable/metrics/`.  Any other
classes have to be relocated
