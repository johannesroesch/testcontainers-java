package org.testcontainers.containers;

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.cql.ResultSet;
import com.datastax.oss.driver.api.core.cql.Row;
import lombok.extern.slf4j.Slf4j;
import org.junit.Test;
import org.testcontainers.containers.wait.Cassandra4QueryWaitStrategy;
import org.testcontainers.utility.DockerImageName;

import java.net.InetSocketAddress;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

/**
 * @author Eugeny Karpov
 */
@Slf4j
public class Cassandra4ContainerTest {

    private static final DockerImageName CASSANDRA_IMAGE = DockerImageName.parse("cassandra:3.11.2");

    private static final String TEST_CLUSTER_NAME_IN_CONF = "Test Cluster Integration Test";

    @Test
    public void testSimple() {
        try (Cassandra4Container<?> cassandraContainer = new Cassandra4Container<>(CASSANDRA_IMAGE)) {
            cassandraContainer.start();
            ResultSet resultSet = performQuery(cassandraContainer, "SELECT release_version FROM system.local");
            assertTrue("Query was not applied", resultSet.wasApplied());
            assertNotNull("Result set has no release_version", resultSet.one().getString(0));
        }
    }

    @Test
    public void testSpecificVersion() {
        String cassandraVersion = "3.0.15";
        try (Cassandra4Container<?> cassandraContainer = new Cassandra4Container<>(CASSANDRA_IMAGE.withTag(cassandraVersion))) {
            cassandraContainer.start();
            ResultSet resultSet = performQuery(cassandraContainer, "SELECT release_version FROM system.local");
            assertTrue("Query was not applied", resultSet.wasApplied());
            assertEquals("Cassandra has wrong version", cassandraVersion, resultSet.one().getString(0));
        }
    }

    @Test
    public void testConfigurationOverride() {
        try (
            Cassandra4Container<?> cassandraContainer = new Cassandra4Container<>(CASSANDRA_IMAGE)
                .withConfigurationOverride("cassandra-test-configuration-example")
        ) {
            cassandraContainer.start();
            ResultSet resultSet = performQuery(cassandraContainer, "SELECT cluster_name FROM system.local");
            assertTrue("Query was not applied", resultSet.wasApplied());
            assertEquals("Cassandra configuration is not overridden", TEST_CLUSTER_NAME_IN_CONF, resultSet.one().getString(0));
        }
    }

    @Test(expected = ContainerLaunchException.class)
    public void testEmptyConfigurationOverride() {
        try (
            Cassandra4Container<?> cassandraContainer = new Cassandra4Container<>(CASSANDRA_IMAGE)
                .withConfigurationOverride("cassandra-empty-configuration")
        ) {
            cassandraContainer.start();
        }
    }

    @Test
    public void testInitScript() {
        try (
            Cassandra4Container<?> cassandraContainer = new Cassandra4Container<>(CASSANDRA_IMAGE)
                .withInitScript("initial.cql")
        ) {
            cassandraContainer.start();
            testInitScript(cassandraContainer);
        }
    }

    @Test
    public void testInitScriptWithLegacyCassandra() {
        try (
            Cassandra4Container<?> cassandraContainer = new Cassandra4Container<>(DockerImageName.parse("cassandra:2.2.11"))
                .withInitScript("initial.cql")
        ) {
            cassandraContainer.start();
            testInitScript(cassandraContainer);
        }
    }

    @SuppressWarnings("deprecation") // Using deprecated constructor for verification of backwards compatibility
    @Test
    public void testCassandraQueryWaitStrategy() {
        try (
            Cassandra4Container<?> cassandraContainer = new Cassandra4Container<>()
                .waitingFor(new Cassandra4QueryWaitStrategy())
        ) {
            cassandraContainer.start();
            ResultSet resultSet = performQuery(cassandraContainer, "SELECT release_version FROM system.local");
            assertTrue("Query was not applied", resultSet.wasApplied());
        }
    }

    @SuppressWarnings("deprecation") // Using deprecated constructor for verification of backwards compatibility
    @Test
    public void testCassandraGetCluster() {
        try (Cassandra4Container<?> cassandraContainer = new Cassandra4Container<>()) {
            cassandraContainer.start();
            ResultSet resultSet = performQuery(cassandraContainer.getCluster(), "SELECT release_version FROM system.local");
            assertTrue("Query was not applied", resultSet.wasApplied());
            assertNotNull("Result set has no release_version", resultSet.one().getString(0));
        }
    }

    private void testInitScript(Cassandra4Container<?> cassandraContainer) {
        ResultSet resultSet = performQuery(cassandraContainer, "SELECT * FROM keySpaceTest.catalog_category");
        assertTrue("Query was not applied", resultSet.wasApplied());
        Row row = resultSet.one();
        assertEquals("Inserted row is not in expected state", 1, row.getLong(0));
        assertEquals("Inserted row is not in expected state", "test_category", row.getString(1));
    }

    private ResultSet performQuery(Cassandra4Container<?> cassandraContainer, String cql) {
        CqlSession explicitCluster = CqlSession.builder()
            .addContactPoint(InetSocketAddress.createUnresolved(cassandraContainer.getHost(), cassandraContainer.getMappedPort(Cassandra4Container.CQL_PORT)))
            .build();
        return performQuery(explicitCluster, cql);
    }

    private ResultSet performQuery(CqlSession session, String cql) {
            return session.execute(cql);

    }
}
