package com.vmturbo.components.test.utilities.metric.scraper;

import java.net.URI;
import java.time.Clock;
import java.util.Collections;
import java.util.List;

import javax.annotation.Nonnull;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.springframework.web.client.RestClientException;

import com.google.common.annotations.VisibleForTesting;

import io.prometheus.client.Collector.MetricFamilySamples;

import com.vmturbo.components.test.utilities.EnvOverrideableProperties;

/**
 * Scrapes Prometheus metrics from a CAdvisor instance. CAdvisor is a tool that scrapes container
 * metrics from the local Docker containers, such as their resource and network usage, resource
 * isolation parameters, etc.
 *
 * For more information see: https://github.com/google/cadvisor/
 */
public class CAdvisorMetricsScraper extends RemoteMetricsScraper {
    /**
     * The port CAdvisor is listening on.
     */
    @VisibleForTesting
    static final String CADVISOR_PORT_PROP = "CADVISOR_PORT";

    private static final EnvOverrideableProperties DEFAULT_PROPS =
            EnvOverrideableProperties.newBuilder()
                    .addProperty(CADVISOR_PORT_PROP, "8787")
                    .build();

    private final Logger logger = LogManager.getLogger();

    /**
     * Whether or not there is a reachable CAdvisor at the time of scraper creation.
     * <p>
     * At the time of this writing (April 18, 2017) the test framework is not going to
     * start up/shut down CAdvisor. It may or may not be running on the test VM.
     */
    private final boolean cadvisorReachable;

    private final EnvOverrideableProperties props;

    @VisibleForTesting
    CAdvisorMetricsScraper(@Nonnull final EnvOverrideableProperties props,
                           @Nonnull final Clock clock) {
        super("cadvisor", clock);
        this.props = props;

        boolean cadvisorUp = false;
        try {
            super.sampleMetrics();
            cadvisorUp = true;
        } catch (RestClientException e) {
            logger.info("Unable to sample local CAdvisor at {} due to error: {}." +
                            " Won't attempt to scrape Node Exporter metrics.", getMetricsUri(),
                    e.getLocalizedMessage());
        }
        cadvisorReachable = cadvisorUp;
    }

    @VisibleForTesting
    boolean isCadvisorReachable() {
        return cadvisorReachable;
    }

    public CAdvisorMetricsScraper(@Nonnull final Clock clock) {
        this(DEFAULT_PROPS, clock);
    }

    @Nonnull
    @Override
    protected List<MetricFamilySamples> sampleMetrics() {
        return cadvisorReachable ? super.sampleMetrics() : Collections.emptyList();
    }

    @Override
    protected URI getMetricsUri() {
        // TODO (roman, April 18 2017): If we convert the ComponentCluster to be runnable on
        // remote hosts then we should have a way to inject the remote host IP into this scraper.
        return URI.create("http://localhost:"
                + props.get(CADVISOR_PORT_PROP) + "/metrics");
    }
}
