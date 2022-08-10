package com.vmturbo.components.test.utilities.metric.scraper;

import java.net.URI;
import java.time.Clock;
import java.util.Collections;
import java.util.List;

import javax.servlet.Servlet;
import javax.servlet.ServletContext;
import javax.servlet.ServletRegistration;

import com.google.common.collect.Lists;

import io.prometheus.client.Collector.MetricFamilySamples;
import io.prometheus.client.CollectorRegistry;
import io.prometheus.client.exporter.MetricsServlet;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestName;
import org.mockito.Mockito;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.mock.env.MockEnvironment;
import org.springframework.web.servlet.config.annotation.EnableWebMvc;

import com.vmturbo.components.api.test.IntegrationTestServer;
import com.vmturbo.components.common.BaseVmtComponent;
import com.vmturbo.components.test.utilities.component.ComponentCluster;
import com.vmturbo.components.test.utilities.metric.MetricTestUtil;

public class ComponentMetricsScrapeTest {

    @Rule
    public TestName testName = new TestName();

    private IntegrationTestServer server;

    private CollectorRegistry testRegistry;

    @Before
    public void startup() throws Exception {
        final MockEnvironment env = new MockEnvironment();
        env.setProperty("instance_id", "test");
        server = new IntegrationTestServer(testName, ContextConfiguration.class, env);
        testRegistry = server.getBean(CollectorRegistry.class);
    }

    @After
    public void cleanup() throws Exception {
        server.close();
    }

    @Test
    public void testRemoteHarvest() throws Exception {
        final MetricFamilySamples testSample = MetricTestUtil.createSimpleFamily("test", 10);
        Mockito.when(testRegistry.filteredMetricFamilySamples(Mockito.anySet()))
                .thenReturn(Collections.enumeration(Lists.newArrayList(testSample)));

        final ComponentCluster rule = Mockito.mock(ComponentCluster.class);
        Mockito.when(rule.getMetricsURI(Mockito.eq("testComponent")))
                .thenReturn(URI.create(
                        "http://localhost:" + server.connectionConfig().getPort() + "/metrics"));
        final ComponentMetricsScraper marketHarvester =
                new ComponentMetricsScraper("testComponent", Clock.systemUTC());
        marketHarvester.initialize(rule);

        final List<MetricFamilySamples> samples = marketHarvester.sampleMetrics();
        Assert.assertEquals(1, samples.size());
        Assert.assertEquals(testSample, samples.get(0));
    }

    @Configuration
    @EnableWebMvc
    public static class ContextConfiguration {

        @Autowired
        private ServletContext servletContext;

        @Bean
        public CollectorRegistry testRegistry() {
            return Mockito.spy(new CollectorRegistry());
        }

        @Bean
        public Servlet metricsServlet() {
            final Servlet servlet = new MetricsServlet(testRegistry());
            final ServletRegistration.Dynamic registration =
                    servletContext.addServlet("metrics-servlet", servlet);
            registration.setLoadOnStartup(1);
            registration.addMapping(BaseVmtComponent.METRICS_URL);
            return servlet;
        }
    }
}
