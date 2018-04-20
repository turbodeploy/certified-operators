package com.vmturbo.components.test.utilities.metric.scraper;

import java.time.Clock;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestName;
import org.mockito.Mockito;
import org.springframework.mock.env.MockEnvironment;

import com.vmturbo.components.api.test.IntegrationTestServer;
import com.vmturbo.components.test.utilities.EnvOverrideableProperties;

public class TestNodeExporterScraper {

    static class ContextConfiguration {
        // An empty spring context means all requests should return 404.
    }

    @Rule
    public TestName testName = new TestName();

    private IntegrationTestServer server;

    @Before
    public void startup() throws Exception {
        final MockEnvironment env = new MockEnvironment();
        env.setProperty("instance_id", "test");
        server = new IntegrationTestServer(testName,
                ComponentMetricsScrapeTest.ContextConfiguration.class, env);
    }

    @After
    public void cleanup() throws Exception {
        server.close();
    }

    @Test
    public void testCadvisorUnavailableNoError() throws Exception {
        EnvOverrideableProperties props = Mockito.mock(EnvOverrideableProperties.class);
        Mockito.when(props.get(Mockito.eq(NodeExporterMetricsScraper.NODE_EXPORTER_PORT_PROP)))
                .thenReturn(Integer.toString(server.connectionConfig().getPort()));
        NodeExporterMetricsScraper scraper =
                new NodeExporterMetricsScraper(props, Clock.systemUTC());
        Assert.assertFalse(scraper.isNodeExporterReachable());
        Assert.assertTrue(scraper.sampleMetrics().isEmpty());
    }
}
