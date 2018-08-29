package com.google.cloud.bigtable.hbase;

/**
 * This class is here to force generation of source javadoc jars so that the maven release process
 * doesn't complain. The shading plugin generated a shaded jar of bigtable-shaded, but it doesn't
 * generate javadoc or source files; this class is here as a hack and better methods should be
 * employed.
 *
 * @author amovalia
 * @version $Id: $Id
 */
public final class MavenPlaceholder {
    private MavenPlaceholder(){

    }
}
