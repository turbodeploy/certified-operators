package com.vmturbo.components.common.diagnostics;

import java.io.IOException;
import java.io.StringWriter;

import javax.annotation.Nonnull;

import io.prometheus.client.CollectorRegistry;
import io.prometheus.client.exporter.common.TextFormat;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

/**
 * Diagnostics provider for Prometheus metrics.
 */
public class PrometheusDiagnosticsProvider implements StringDiagnosable {

    private static final String PROMETHEUS_METRICS_FILE_NAME = "PrometheusMetrics";
    private final Logger logger = LogManager.getLogger(getClass());

    private final CollectorRegistry collectorRegistry;

    /**
     * Constructs the diagnostics provider.
     *
     * @param collectorRegistry collector registry to get data from
     */
    public PrometheusDiagnosticsProvider(@Nonnull final CollectorRegistry collectorRegistry) {
        this.collectorRegistry = collectorRegistry;
    }

    @Nonnull
    @Override
    public String getFileName() {
        return PROMETHEUS_METRICS_FILE_NAME;
    }

    @Override
    public void collectDiags(@Nonnull DiagnosticsAppender sink) throws DiagnosticsException {
        try (StringWriter writer = new StringWriter()) {
            logger.info("Creating prometheus log entry " + PROMETHEUS_METRICS_FILE_NAME);
            TextFormat.write004(writer, collectorRegistry.metricFamilySamples());
            sink.appendString(writer.toString());
        } catch (IOException e) {
            logger.error(e);
            throw new DiagnosticsException("Error writing Prometheus metrics.", e);
        }
    }
}
