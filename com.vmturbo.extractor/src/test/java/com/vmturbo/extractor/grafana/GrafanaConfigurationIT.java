package com.vmturbo.extractor.grafana;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.Collections;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.jooq.SQLDialect;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Rule;
import org.junit.Test;

import com.vmturbo.auth.api.db.DBPasswordUtil;
import com.vmturbo.auth.api.licensing.LicenseCheckClient;
import com.vmturbo.components.api.test.ResourcePath;
import com.vmturbo.components.test.utilities.ComponentTestRule;
import com.vmturbo.components.test.utilities.component.ComponentCluster;
import com.vmturbo.components.test.utilities.component.ServiceHealthCheck.BasicServiceHealthCheck;
import com.vmturbo.extractor.ExtractorGlobalConfig.ExtractorFeatureFlags;
import com.vmturbo.extractor.grafana.Grafanon.GrafanonConfig;
import com.vmturbo.extractor.grafana.client.GrafanaClient;
import com.vmturbo.extractor.grafana.client.GrafanaClientConfig;
import com.vmturbo.sql.utils.DbEndpoint;
import com.vmturbo.sql.utils.DbEndpoint.DbEndpointCompleter;
import com.vmturbo.sql.utils.DbEndpointConfig;

/**
 * An integration test which stands up a local Grafana instance, and verifies that the grafana
 * configuration logic uploaded things correctly.
 */
public class GrafanaConfigurationIT {

    private static final Logger logger = LogManager.getLogger();

    /**
     * Stands up the "grafana" instance.
     */
    @Rule
    public ComponentTestRule testRule = ComponentTestRule.newBuilder()
        .withComponentCluster(ComponentCluster.newBuilder()
            .withService(ComponentCluster.newService("grafana")
                .withHealthCheck(new BasicServiceHealthCheck())
                .logsToLogger(logger)
                .build())
            .disableClusterMgr())
        .withoutStubs()
        .noMetricsCollection();

    private GrafanaClient grafanaClient;
    private Grafanon grafanon;
    private DashboardsOnDisk dashboardsOnDisk;
    private DbEndpoint dbendpointMock;

    /**
     * Common code to run before the test. Initializes all the dependencies.
     */
    @Before
    public void setup() {
        final GrafanaClientConfig clientConfig = new GrafanaClientConfig()
                .setGrafanaHost("localhost")
                // TODO - dynamic port so we don't crash if there is a local Grafana instance running.
                .setGrafanaPort(3000)
                .setAdminUser("admin")
                .setAdminPassword("admin");
        grafanaClient = new GrafanaClient(clientConfig);

        dashboardsOnDisk = new DashboardsOnDisk(
                ResourcePath.getTestResource(DashboardsOnDisk.class, "dashboards").toString());

        final Map<String, String> m = Collections.emptyMap();
        final DbEndpointCompleter endpointCompleter = new DbEndpointCompleter(m::get, mock(DBPasswordUtil.class));
        final DbEndpointConfig endpointConfig =
            endpointCompleter.newEndpointBuilder("extractor", SQLDialect.POSTGRES)
                .withDbDatabaseName("mydb")
                .withDbUserName("me")
                .withDbPassword("foo")
                .withDbPort(300)
                .withDbEndpointEnabled(false)
                .build().getConfig();

        GrafanonConfig config = new GrafanonConfig(() -> endpointConfig)
                .setTimescaleDisplayName("Test Endpoint")
                .setErrorSleepInterval(10, TimeUnit.SECONDS);

        ExtractorFeatureFlags extractorFeatureFlags = mock(ExtractorFeatureFlags.class);
        when(extractorFeatureFlags.isReportingEnabled()).thenReturn(true);
        dbendpointMock = mock(DbEndpoint.class);
        LicenseCheckClient licenseCheckClient = mock(LicenseCheckClient.class);
        grafanon = new Grafanon(config, dashboardsOnDisk, grafanaClient, extractorFeatureFlags, dbendpointMock, licenseCheckClient);
    }

    /**
     * Test saving dashboards, and verify that the dashboards got uploaded correctly.
     *
     * @throws Exception If there is an error.
     */
    @Test
    @Ignore
    public void testSaveAndLoadDashboards() throws Exception {
        final RefreshSummary refreshSummary = new RefreshSummary();
        grafanon.initialize();
        grafanon.getInitializationFuture().get(1, TimeUnit.MINUTES);
        logger.info(refreshSummary);

        final Set<String> expectedUids = new HashSet<>();
        dashboardsOnDisk.visit(folderData ->
                expectedUids.addAll(folderData.getDashboardsByUid().keySet()));

        Map<String, Long> dashboards = grafanaClient.dashboardIdsByUid();
        assertThat(dashboards.keySet(), is(expectedUids));
    }
}
