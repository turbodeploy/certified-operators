package com.vmturbo.clustermgr.management;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.sql.SQLException;
import java.time.LocalDateTime;
import java.time.temporal.ChronoUnit;
import java.util.concurrent.TimeUnit;

import com.google.common.collect.Table;

import org.jooq.DSLContext;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Configuration;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.annotation.DirtiesContext.ClassMode;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.TestPropertySource;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

import com.vmturbo.clustermgr.ClustermgrDBConfig2;
import com.vmturbo.clustermgr.db.Clustermgr;
import com.vmturbo.clustermgr.db.Tables;
import com.vmturbo.clustermgr.db.tables.records.RegisteredComponentRecord;
import com.vmturbo.clustermgr.management.ComponentRegistryTest.TestClustermgrDBConfig2;
import com.vmturbo.common.protobuf.cluster.ComponentStatus.ComponentIdentifier;
import com.vmturbo.common.protobuf.cluster.ComponentStatus.ComponentInfo;
import com.vmturbo.common.protobuf.cluster.ComponentStatus.ComponentStarting;
import com.vmturbo.common.protobuf.cluster.ComponentStatus.UriInfo;
import com.vmturbo.components.api.test.MutableFixedClock;
import com.vmturbo.components.common.featureflags.FeatureFlags;
import com.vmturbo.sql.utils.DbCleanupRule;
import com.vmturbo.sql.utils.DbConfigurationRule;
import com.vmturbo.sql.utils.DbEndpoint;
import com.vmturbo.sql.utils.DbEndpoint.UnsupportedDialectException;
import com.vmturbo.sql.utils.DbEndpointTestRule;
import com.vmturbo.test.utils.FeatureFlagTestRule;

/**
 * Unit tests for {@link ComponentRegistry}.
 */
@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration(classes = {TestClustermgrDBConfig2.class})
@DirtiesContext(classMode = ClassMode.BEFORE_CLASS)
@TestPropertySource(properties = {"dbPort=3306", "dbRootPassword=vmturbo", "sqlDialect=MARIADB"})
public class ComponentRegistryTest {

    @Autowired(required = false)
    private TestClustermgrDBConfig2 dbConfig;

    private static final UriInfo URI_INFO = UriInfo.newBuilder()
        .setRoute("route")
        .setIpAddress("ip")
        .setPort(123)
        .build();

    private static final long UNHEALTHY_DEREGISTRATION_SEC = 100;

    /**
     * Rule to set up the database before running the tests.
     */
    @ClassRule
    public static DbConfigurationRule dbConfigurationRule = new DbConfigurationRule(Clustermgr.CLUSTERMGR);

    /**
     * Rule to clean up the database after each test.
     */
    @Rule
    public DbCleanupRule dbCleanupRule = dbConfigurationRule.cleanupRule();

    /**
     * Test rule to use {@link DbEndpoint}s in test.
     */
    @Rule
    public DbEndpointTestRule dbEndpointTestRule = new DbEndpointTestRule("clustermgr");

    /**
     * Rule to manage feature flag enablement to make sure FeatureFlagManager store is set up.
     */
    @Rule
    public FeatureFlagTestRule featureFlagTestRule = new FeatureFlagTestRule().testAllCombos(
            FeatureFlags.POSTGRES_PRIMARY_DB);

    private DSLContext dsl;

    private MutableFixedClock clock = new MutableFixedClock(1_000_000);
    private ComponentRegistry componentRegistry;

    /**
     * Set up before each test.
     *
     * @throws SQLException if there is db error
     * @throws UnsupportedDialectException if the dialect is not supported
     * @throws InterruptedException if interrupted
     */
    @Before
    public void before() throws SQLException, UnsupportedDialectException, InterruptedException {
        if (FeatureFlags.POSTGRES_PRIMARY_DB.isEnabled()) {
            dbEndpointTestRule.addEndpoints(dbConfig.clusterMgrEndpoint());
            dsl = dbConfig.clusterMgrEndpoint().dslContext();
        } else {
            dsl = dbConfigurationRule.getDslContext();
        }
        componentRegistry = new ComponentRegistry(dsl, clock, UNHEALTHY_DEREGISTRATION_SEC, TimeUnit.SECONDS);
    }

    /**
     * Test registering components and retrieving registered components.
     *
     * @throws Exception To satisfy compiler.
     */
    @Test
    public void testRegisterAndRetrieve() throws Exception {
        final ComponentStarting fooInstance1 = newComponentStarting("foo", "instance1", 1L);
        final ComponentStarting fooInstance2 = newComponentStarting("foo", "instance2", 2L);
        final ComponentStarting barInstance1 = newComponentStarting("bar", "instance3", 3L);
        assertThat(componentRegistry.registerComponent(fooInstance1), is(fooInstance1.getComponentInfo()));
        assertThat(componentRegistry.registerComponent(fooInstance2), is(fooInstance2.getComponentInfo()));
        assertThat(componentRegistry.registerComponent(barInstance1), is(barInstance1.getComponentInfo()));

        final Table<String, String, RegisteredComponent> registeredComponents = componentRegistry.getRegisteredComponents();
        assertThat(registeredComponents.size(), is(3));
        final RegisteredComponent registeredFooInstance1 = registeredComponents.get("foo", "instance1");
        final RegisteredComponent registeredFooInstance2 = registeredComponents.get("foo", "instance2");
        final RegisteredComponent registeredBarInstance1 = registeredComponents.get("bar", "instance3");

        assertThat(registeredFooInstance1.getComponentHealth(), is(ComponentHealth.UNKNOWN));
        assertThat(registeredFooInstance1.getComponentInfo(), is(fooInstance1.getComponentInfo()));
        assertThat(registeredFooInstance2.getComponentHealth(), is(ComponentHealth.UNKNOWN));
        assertThat(registeredFooInstance2.getComponentInfo(), is(fooInstance2.getComponentInfo()));
        assertThat(registeredBarInstance1.getComponentHealth(), is(ComponentHealth.UNKNOWN));
        assertThat(registeredBarInstance1.getComponentInfo(), is(barInstance1.getComponentInfo()));

        // Check the times.
        dsl.selectFrom(Tables.REGISTERED_COMPONENT)
            .fetch()
            .forEach(record -> {
                assertThat(record.getRegistrationTime(), is(LocalDateTime.now(clock)));
                assertThat(record.getLastUpdateTime(), is(LocalDateTime.now(clock)));
                assertThat(record.getLastStatusChangeTime(), is(LocalDateTime.now(clock)));
            });
    }

    /**
     * Test registering components and retrieving registered components.
     *
     * @throws Exception To satisfy compiler.
     */
    @Test
    public void testRegisterDeregistersExistingInstance() throws Exception {
        final ComponentStarting fooInstance1 = newComponentStarting("foo", "instance1", 1L);
        final ComponentStarting fooInstance1DiffJvm = newComponentStarting("foo", "instance1", 2L);
        assertThat(componentRegistry.registerComponent(fooInstance1), is(fooInstance1.getComponentInfo()));
        assertThat(componentRegistry.registerComponent(fooInstance1DiffJvm), is(fooInstance1DiffJvm.getComponentInfo()));

        final Table<String, String, RegisteredComponent> registeredComponents = componentRegistry.getRegisteredComponents();
        // Just one - the different JVM overrides the first.
        assertThat(registeredComponents.size(), is(1));
        final RegisteredComponent registeredFooInstance1 = registeredComponents.get("foo", "instance1");
        assertThat(registeredFooInstance1.getComponentHealth(), is(ComponentHealth.UNKNOWN));
        assertThat(registeredFooInstance1.getComponentInfo(), is(fooInstance1DiffJvm.getComponentInfo()));
    }

    /**
     * Test deregistering components.
     *
     * @throws Exception To satisfy compiler.
     */
    @Test
    public void testDeregister() throws Exception {
        final ComponentStarting fooInstance1 = newComponentStarting("foo", "instance1", 1L);
        assertThat(componentRegistry.registerComponent(fooInstance1), is(fooInstance1.getComponentInfo()));

        assertFalse(componentRegistry.getRegisteredComponents().isEmpty());
        assertTrue(componentRegistry.deregisterComponent(fooInstance1.getComponentInfo().getId()));
        assertTrue(componentRegistry.getRegisteredComponents().isEmpty());
    }

    /**
     * Test deregistering a non-existing component.
     *
     * @throws Exception To satisfy compiler.
     */
    @Test
    public void testDeregisterUnexisting() throws Exception {
        final ComponentStarting fooInstance1 = newComponentStarting("foo", "instance1", 1L);
        assertFalse(componentRegistry.deregisterComponent(fooInstance1.getComponentInfo().getId()));
    }

    /**
     * Test updating a component's health status from the initial "unknown" to something.
     *
     * @throws Exception To satisfy compiler.
     */
    @Test
    public void testUpdateStatusFirstTime() throws Exception {
        final ComponentStarting fooInstance1 = newComponentStarting("foo", "instance1", 1L);
        assertThat(componentRegistry.registerComponent(fooInstance1), is(fooInstance1.getComponentInfo()));

        // Add some time so we can check that the last update times get set correctly.
        clock.addTime(10, ChronoUnit.SECONDS);

        final String statusDesc = "I am healthy!";
        componentRegistry.updateComponentHealthStatus(fooInstance1.getComponentInfo().getId(), ComponentHealth.HEALTHY, statusDesc);

        final Table<String, String, RegisteredComponent> registeredComponents = componentRegistry.getRegisteredComponents();
        assertThat(registeredComponents.get("foo", "instance1").getComponentHealth(), is(ComponentHealth.HEALTHY));
        // We don't retrieve the status descriptions because we don't need them, but make sure
        // they're up to date.
        final RegisteredComponentRecord record = dsl.selectFrom(Tables.REGISTERED_COMPONENT)
            .where(Tables.REGISTERED_COMPONENT.INSTANCE_ID.eq("instance1"))
            .fetchOne();
        assertThat(record.getStatusDescription(), is(statusDesc));
        assertThat(record.getLastStatusChangeTime(), is(LocalDateTime.now(clock)));
        assertThat(record.getLastUpdateTime(), is(LocalDateTime.now(clock)));
    }

    /**
     * Test updating a component's health status to the same status.
     *
     * @throws Exception To satisfy compiler.
     */
    @Test
    public void testUpdateStatusSameStatus() throws Exception {
        final ComponentStarting fooInstance1 = newComponentStarting("foo", "instance1", 1L);
        assertThat(componentRegistry.registerComponent(fooInstance1), is(fooInstance1.getComponentInfo()));

        clock.addTime(1, ChronoUnit.SECONDS);

        componentRegistry.updateComponentHealthStatus(fooInstance1.getComponentInfo().getId(), ComponentHealth.HEALTHY, "I am healthy!");
        final LocalDateTime initialHealthyStatusTime = LocalDateTime.now(clock);

        clock.addTime(1, ChronoUnit.SECONDS);

        final String finalStatusDesc = "I am STILL healthy!";
        componentRegistry.updateComponentHealthStatus(fooInstance1.getComponentInfo().getId(), ComponentHealth.HEALTHY, finalStatusDesc);

        final Table<String, String, RegisteredComponent> registeredComponents = componentRegistry.getRegisteredComponents();
        assertThat(registeredComponents.get("foo", fooInstance1.getComponentInfo().getId().getInstanceId()).getComponentHealth(), is(ComponentHealth.HEALTHY));
        // We don't retrieve the status descriptions because we don't need them, but make sure
        // they're up to date.
        final RegisteredComponentRecord record = dsl.selectFrom(Tables.REGISTERED_COMPONENT)
            .where(Tables.REGISTERED_COMPONENT.INSTANCE_ID.eq("instance1"))
            .fetchOne();
        assertThat(record.getStatusDescription(), is(finalStatusDesc));
        assertThat(record.getLastStatusChangeTime(), is(initialHealthyStatusTime));
        assertThat(record.getLastUpdateTime(), is(LocalDateTime.now(clock)));
    }

    /**
     * Test updating a component status to a different status.
     *
     * @throws Exception To satisfy compiler.
     */
    @Test
    public void testUpdateStatusDifferentStatus() throws Exception {
        final ComponentStarting fooInstance1 = newComponentStarting("foo", "instance1", 1L);
        assertThat(componentRegistry.registerComponent(fooInstance1), is(fooInstance1.getComponentInfo()));

        clock.addTime(1, ChronoUnit.SECONDS);

        // Initial status is critical.
        componentRegistry.updateComponentHealthStatus(fooInstance1.getComponentInfo().getId(), ComponentHealth.CRITICAL, "I am not healthy!");
        // Make sure the status is correct initially.
        assertThat(componentRegistry.getRegisteredComponents().get("foo", fooInstance1.getComponentInfo().getId().getInstanceId()).getComponentHealth(), is(ComponentHealth.CRITICAL));

        clock.addTime(1, ChronoUnit.SECONDS);

        final String finalStatusDesc = "I am now healthy!";
        componentRegistry.updateComponentHealthStatus(fooInstance1.getComponentInfo().getId(), ComponentHealth.HEALTHY, finalStatusDesc);
        final Table<String, String, RegisteredComponent> registeredComponents = componentRegistry.getRegisteredComponents();
        assertThat(registeredComponents.get("foo", fooInstance1.getComponentInfo().getId().getInstanceId()).getComponentHealth(), is(ComponentHealth.HEALTHY));
        // We don't retrieve the status descriptions because we don't need them, but make sure
        // they're up to date.
        final RegisteredComponentRecord record = dsl.selectFrom(Tables.REGISTERED_COMPONENT)
            .where(Tables.REGISTERED_COMPONENT.INSTANCE_ID.eq("instance1"))
            .fetchOne();
        assertThat(record.getStatusDescription(), is(finalStatusDesc));
        assertThat(record.getLastStatusChangeTime(), is(LocalDateTime.now(clock)));
        assertThat(record.getLastUpdateTime(), is(LocalDateTime.now(clock)));
    }

    /**
     * Test that a component deregisters if it's in critical condition for too long.
     *
     * @throws Exception To satisfy compiler.
     */
    @Test
    public void testUpdateStatusDeregisterAfterInterval() throws Exception {
        final ComponentStarting fooInstance1 = newComponentStarting("foo", "instance1", 1L);
        assertThat(componentRegistry.registerComponent(fooInstance1), is(fooInstance1.getComponentInfo()));

        // Initial status is critical.
        componentRegistry.updateComponentHealthStatus(fooInstance1.getComponentInfo().getId(), ComponentHealth.CRITICAL, "I am not healthy!");
        assertThat(componentRegistry.getRegisteredComponents().size(), is(1));

        clock.addTime(UNHEALTHY_DEREGISTRATION_SEC - 1, ChronoUnit.SECONDS);
        componentRegistry.updateComponentHealthStatus(fooInstance1.getComponentInfo().getId(), ComponentHealth.CRITICAL, "I am still not healthy!");
        assertThat(componentRegistry.getRegisteredComponents().size(), is(1));

        clock.addTime(1, ChronoUnit.SECONDS);
        componentRegistry.updateComponentHealthStatus(fooInstance1.getComponentInfo().getId(), ComponentHealth.CRITICAL, "I am STILL not healthy!");
        assertThat(componentRegistry.getRegisteredComponents().size(), is(0));
    }

    private ComponentStarting newComponentStarting(String componentType, String instanceId, long jvmId) {
        return ComponentStarting.newBuilder()
                .setComponentInfo(ComponentInfo.newBuilder()
                        .setId(ComponentIdentifier.newBuilder()
                                .setComponentType(componentType)
                                .setInstanceId(instanceId)
                                .setJvmId(jvmId))
                        .setUriInfo(URI_INFO))
                .build();
    }

    /**
     * Workaround for ClustermgrDBConfig2 (remove conditional annotation), since it's conditionally
     * initialized based on {@link FeatureFlags#POSTGRES_PRIMARY_DB}. When we test all combinations
     * of it using {@link FeatureFlagTestRule}, first it's false, so ClustermgrDBConfig2 is not
     * created; then second it's true, ClustermgrDBConfig2 is created, but the endpoint inside is
     * also eagerly initialized due to the same FF, which results in several issues like: it
     * doesn't go through DbEndpointTestRule, making call to auth to get root password, etc.
     */
    @Configuration
    static class TestClustermgrDBConfig2 extends ClustermgrDBConfig2 {}
}