package com.google.cloud.bigtable.hbase;

import org.junit.runner.RunWith;
import org.junit.runners.Suite;

@RunWith(Suite.class)
@Suite.SuiteClasses({
    TestReplicationAppProfile.class
})
public class IntegrationTestsReplication extends IntegrationTests {
}
