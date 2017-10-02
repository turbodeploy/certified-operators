package com.vmturbo.components.test.utilities;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.List;
import java.util.stream.Collectors;

import org.junit.Before;
import org.junit.Test;

import com.google.common.collect.Sets;

import com.vmturbo.components.test.utilities.communication.ComponentStubHost;
import com.vmturbo.components.test.utilities.component.ComponentCluster;
import com.vmturbo.components.test.utilities.metric.MetricsWarehouse;
import com.vmturbo.components.test.utilities.metric.MetricsWarehouseVisitor;
import com.vmturbo.components.test.utilities.metric.scraper.MetricsScraper;

/**
 * Unit tests for {@link ComponentTestRule}.
 */
public class ComponentTestRuleUtilitiesTest {
    private final ComponentCluster.Builder clusterBuilder = mock(ComponentCluster.Builder.class);
    private final ComponentCluster cluster = mock(ComponentCluster.class);

    @Before
    public void setup() {
        when(clusterBuilder.build()).thenReturn(cluster);
        when(cluster.getComponents()).thenReturn(Sets.newHashSet("repository", "arangodb"));
    }

    @Test
    public void testScrapeClusterAndLocalMetrics() {
        // Should scrape both arangodb and repository along with the defaults of local, cadvisor, node_exporter
        final ComponentTestRule rule = ComponentTestRule.newBuilder()
            .withComponentCluster(clusterBuilder)
            .withStubs(ComponentStubHost.newBuilder())
            .scrapeClusterAndLocalMetrics(mock(MetricsWarehouseVisitor.class));

        final MetricsWarehouse metricsWarehouse = rule.getMetricsWarehouse().get();
        List<String> scrapedComponents = metricsWarehouse.getMetricsScrapers().stream()
            .map(MetricsScraper::getName)
            .collect(Collectors.toList());

        assertThat(scrapedComponents,
            containsInAnyOrder("local", "cadvisor", "node_exporter", "arangodb", "repository"));
    }

    @Test
    public void testScrapeServicesAndLocalMetrics() {
        // Should not scrape arangodb
        final ComponentTestRule rule = ComponentTestRule.newBuilder()
            .withComponentCluster(clusterBuilder)
            .withStubs(ComponentStubHost.newBuilder())
            .scrapeServicesAndLocalMetrics(mock(MetricsWarehouseVisitor.class), "repository");

        final MetricsWarehouse metricsWarehouse = rule.getMetricsWarehouse().get();
        List<String> scrapedComponents = metricsWarehouse.getMetricsScrapers().stream()
            .map(MetricsScraper::getName)
            .collect(Collectors.toList());

        assertThat(scrapedComponents,
            containsInAnyOrder("local", "cadvisor", "node_exporter", "repository"));
    }
}
