package com.vmturbo.components.test.utilities.metric.scraper;

import java.time.Clock;

import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mockito;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.context.embedded.LocalServerPort;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.context.SpringBootTest.WebEnvironment;
import org.springframework.test.context.junit4.SpringRunner;

import com.vmturbo.components.test.utilities.EnvOverrideableProperties;

@RunWith(SpringRunner.class)
@SpringBootTest(webEnvironment= WebEnvironment.RANDOM_PORT,
        properties={"spring.cloud.consul.config.enabled=false","instance_id=test"})
public class TestNodeExporterScraper {

    @SpringBootApplication
    static class ContextConfiguration {
        // An empty spring context means all requests should return 404.
    }

    @LocalServerPort
    private int localPort;

    @Test
    public void testCadvisorUnavailableNoError() {
        EnvOverrideableProperties props = Mockito.mock(EnvOverrideableProperties.class);
        Mockito.when(props.get(Mockito.eq(NodeExporterMetricsScraper.NODE_EXPORTER_PORT_PROP)))
                .thenReturn(Integer.toString(localPort));
        NodeExporterMetricsScraper scraper = new NodeExporterMetricsScraper(props, Clock.systemUTC());
        Assert.assertFalse(scraper.isNodeExporterReachable());
        Assert.assertTrue(scraper.sampleMetrics().isEmpty());
    }
}
