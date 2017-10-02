package com.vmturbo.components.test.utilities.metric.scraper;

import java.time.Clock;
import java.util.Collections;
import java.util.List;

import javax.annotation.Nonnull;

import com.google.common.annotations.VisibleForTesting;

import io.prometheus.client.Collector.MetricFamilySamples;
import io.prometheus.client.CollectorRegistry;

/**
 * Collects metrics from a {@link CollectorRegistry} in the local JVM. The intended use case
 * is to collect metrics from the test framework itself, to keep note of error counts,
 * durations of, for example, bringing up components, etc.
 */
public class LocalMetricsScraper extends MetricsScraper {

    private final CollectorRegistry registry;

    @VisibleForTesting
    public LocalMetricsScraper(@Nonnull final CollectorRegistry registry,
                               @Nonnull final Clock clock) {
        super("local", clock);
        this.registry = registry;
    }

    @Override
    @Nonnull
    protected List<MetricFamilySamples> sampleMetrics() {
        return Collections.list(registry.metricFamilySamples());
    }
}
