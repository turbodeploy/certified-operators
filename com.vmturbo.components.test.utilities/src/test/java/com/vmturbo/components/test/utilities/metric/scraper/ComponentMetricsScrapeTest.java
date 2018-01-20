package com.vmturbo.components.test.utilities.metric.scraper;

import java.net.URI;
import java.time.Clock;
import java.util.Collections;
import java.util.List;

import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mockito;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.context.embedded.LocalServerPort;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.context.SpringBootTest.WebEnvironment;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.boot.web.servlet.ServletRegistrationBean;
import org.springframework.context.annotation.Bean;
import org.springframework.test.context.junit4.SpringRunner;

import io.prometheus.client.Collector.MetricFamilySamples;
import io.prometheus.client.CollectorRegistry;
import io.prometheus.client.exporter.MetricsServlet;

import com.vmturbo.components.test.utilities.component.ComponentCluster;
import com.vmturbo.components.test.utilities.metric.MetricTestUtil;

@RunWith(SpringRunner.class)
@SpringBootTest(webEnvironment= WebEnvironment.RANDOM_PORT,
        properties={"spring.cloud.consul.config.enabled=false","instance_id=test"})
public class ComponentMetricsScrapeTest {

    @SpringBootApplication
    // The default @EnableAutoConfiguration implied by @SpringBootApplication will pick up
    // the spring security JAR in the class-path, and set up authentication for the
    // test application. We want to avoid dealing with that for the purpose of the test, so
    // we explicitly exclude security-related AutoConfiguration.
    @EnableAutoConfiguration(exclude = {
        org.springframework.boot.autoconfigure.security.SecurityAutoConfiguration.class,
        org.springframework.boot.actuate.autoconfigure.ManagementWebSecurityAutoConfiguration.class
    })
    static class ContextConfiguration {

        @Bean
        public CollectorRegistry testRegistry() {
            return new CollectorRegistry();
        }

        @Bean
        public ServletRegistrationBean metricsServlet() {
            return new ServletRegistrationBean(new MetricsServlet(testRegistry()), "/metrics");
        }
    }

    @LocalServerPort
    private int localPort;

    @MockBean
    private CollectorRegistry testRegistry;

    @Test
    public void testRemoteHarvest() {
        final MetricFamilySamples testSample =
                MetricTestUtil.createSimpleFamily("test", 10);
        Mockito.when(testRegistry.metricFamilySamples()).thenReturn(
                Collections.enumeration(Collections.singletonList(testSample)));

        final ComponentCluster rule = Mockito.mock(ComponentCluster.class);
        Mockito.when(rule.getMetricsURI(Mockito.eq("testComponent")))
               .thenReturn(URI.create("http://localhost:" + localPort + "/metrics"));
        final ComponentMetricsScraper marketHarvester =
                new ComponentMetricsScraper("testComponent", Clock.systemUTC());
        marketHarvester.initialize(rule);

        final List<MetricFamilySamples> samples = marketHarvester.sampleMetrics();
        Assert.assertEquals(1, samples.size());
        Assert.assertEquals(testSample, samples.get(0));
    }
}
