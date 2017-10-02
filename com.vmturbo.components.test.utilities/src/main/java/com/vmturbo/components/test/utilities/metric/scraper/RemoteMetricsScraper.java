package com.vmturbo.components.test.utilities.metric.scraper;

import java.io.StringReader;
import java.net.URI;
import java.time.Clock;
import java.util.List;
import java.util.Set;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpMethod;
import org.springframework.http.ResponseEntity;
import org.springframework.web.client.RestTemplate;

import com.google.common.collect.Sets;

import io.prometheus.client.Collector.MetricFamilySamples;
import io.prometheus.client.exporter.common.TextFormat;

import com.vmturbo.components.test.utilities.metric.TextParser;
import com.vmturbo.components.test.utilities.metric.TextParser.TextParseException;

/**
 * The {@link RemoteMetricsScraper} collects prometheus metrics from an HTTP endpoint. The metrics
 * must be in Prometheus' text format.
 */
public abstract class RemoteMetricsScraper extends MetricsScraper {

    private final RestTemplate restTemplate;

    protected RemoteMetricsScraper(@Nonnull final String name,
                                @Nonnull final Clock clock) {
        super(name, clock);
        this.restTemplate = new RestTemplate();
    }

    @Nonnull
    @Override
    protected List<MetricFamilySamples> sampleMetrics() {
        final ResponseEntity<String> response =
                restTemplate.exchange(getMetricsUri(), HttpMethod.GET, null, String.class);
        try {
            return TextParser.parse004(new StringReader(response.getBody()));
        } catch (TextParseException e) {
            // This can only happen if the remote endpoint isn't formatting the metrics in a way
            // that's parseable in the expected format (or if there's a bug :)). Either way, it's
            // a runtime error that shouldn't happen.
            throw new IllegalStateException("Error parsing results from metrics endpoint.", e);
        }
    }

    protected abstract URI getMetricsUri();
}
