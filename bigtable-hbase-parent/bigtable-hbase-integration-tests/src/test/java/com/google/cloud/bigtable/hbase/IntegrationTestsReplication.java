package com.google.cloud.bigtable.hbase;

import org.junit.runner.RunWith;
import org.junit.runners.Suite;

/**
 * A suite of tests that require the target project to be whitelisted for replication
 */
@RunWith(Suite.class)
@Suite.SuiteClasses({
    TestReplicationAppProfile.class
})
public class IntegrationTestsReplication extends IntegrationTests {
}
