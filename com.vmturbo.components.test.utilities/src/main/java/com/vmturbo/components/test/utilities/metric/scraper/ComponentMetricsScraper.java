package com.vmturbo.components.test.utilities.metric.scraper;

import java.net.URI;
import java.time.Clock;

import javax.annotation.Nonnull;

import com.vmturbo.components.test.utilities.component.ComponentCluster;

/**
 * Scrapes metrics from an XL component.
 */
public class ComponentMetricsScraper extends RemoteMetricsScraper {

    private final String componentName;

    private URI metricsUri;

    public ComponentMetricsScraper(@Nonnull final String componentName, @Nonnull final Clock clock) {
        super(componentName, clock);
        this.componentName = componentName;
    }

    @Override
    public void initialize(@Nonnull final ComponentCluster componentCluster) {
        metricsUri = componentCluster.getMetricsURI(componentName);
    }

    @Override
    protected URI getMetricsUri() {
        return metricsUri;
    }
}
