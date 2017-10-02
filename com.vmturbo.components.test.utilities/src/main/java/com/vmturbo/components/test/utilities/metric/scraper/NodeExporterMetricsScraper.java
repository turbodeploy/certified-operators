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
 * Scrapes Prometheus metrics from a Node Exporter instance. NodeExporter is a process that scrapes
 * OS-level performance metrics (memory, file system, network usage, etc) from the host OS.
 *
 * For more information see: https://github.com/prometheus/node_exporter
 */
public class NodeExporterMetricsScraper extends RemoteMetricsScraper {

    /**
     * The port NodeExporter is listening on.
     */
    @VisibleForTesting
    static final String NODE_EXPORTER_PORT_PROP = "NODE_EXPORTER_PORT";

    private final static EnvOverrideableProperties DEFAULT_PROPS =
            EnvOverrideableProperties.newBuilder()
                    .addProperty(NODE_EXPORTER_PORT_PROP, "9100")
                    .build();

    private final Logger logger = LogManager.getLogger();

    /**
     * Whether or not there is a reachable Node Exporter at the time of scraper creation.
     * <p>
     * At the time of this writing (April 18, 2017) the test framework is not going to
     * start up/shut down NodeExporter. It may or may not be running on the test VM. */
    private final boolean nodeExporterReachable;

    private final EnvOverrideableProperties props;


    @VisibleForTesting
    NodeExporterMetricsScraper(@Nonnull final EnvOverrideableProperties props,
                               @Nonnull final Clock clock) {
        super("node_exporter", clock);
        this.props = props;
        boolean exporterUp = false;
        try {
            super.sampleMetrics();
            exporterUp = true;
        } catch (RestClientException e) {
            logger.info("Unable to sample local Node Exporter at {} due to error: {}." +
                            " Won't attempt to scrape Node Exporter metrics.", getMetricsUri(),
                    e.getLocalizedMessage());
        }
        nodeExporterReachable = exporterUp;
    }

    public NodeExporterMetricsScraper(@Nonnull final Clock clock) {
        this(DEFAULT_PROPS, clock);
    }

    @VisibleForTesting
    boolean isNodeExporterReachable() {
        return nodeExporterReachable;
    }

    @Nonnull
    @Override
    protected List<MetricFamilySamples> sampleMetrics() {
        return nodeExporterReachable ? super.sampleMetrics() : Collections.emptyList();
    }

    @Override
    protected URI getMetricsUri() {
        // TODO (roman, April 18 2017): If we convert the ComponentCluster to be runnable on
        // remote hosts then we should have a way to inject the remote host IP into this scraper.
        return URI.create("http://localhost:"
                + props.get(NODE_EXPORTER_PORT_PROP) + "/metrics");
    }
}
