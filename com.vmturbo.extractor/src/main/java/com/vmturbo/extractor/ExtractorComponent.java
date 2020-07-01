package com.vmturbo.extractor;

import java.sql.SQLException;
import java.time.temporal.ChronoUnit;
import java.util.zip.ZipOutputStream;

import javax.annotation.Nonnull;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;

import com.vmturbo.components.common.BaseVmtComponent;
import com.vmturbo.components.common.health.sql.PostgreSQLHealthMonitor;
import com.vmturbo.extractor.diags.ExtractorDiagnosticsConfig;
import com.vmturbo.extractor.grafana.GrafanaConfig;
import com.vmturbo.extractor.schema.ExtractorDbConfig;
import com.vmturbo.extractor.topology.TopologyListenerConfig;
import com.vmturbo.sql.utils.DbEndpoint.UnsupportedDialectException;

/**
 * The Extractor component receiving information broadcast from various components in the system. It
 * writes data to timescaledb database for query, export, and search/sort/filter.
 */
@Configuration("theComponent")
@Import({TopologyListenerConfig.class,
        ExtractorDbConfig.class,
        GrafanaConfig.class,
        ExtractorDiagnosticsConfig.class})
public class ExtractorComponent extends BaseVmtComponent {
    private static final Logger logger = LogManager.getLogger();

    @Autowired
    private ExtractorDiagnosticsConfig diagnosticsConfig;

    @Autowired
    private TopologyListenerConfig listenerConfig;

    @Autowired
    private ExtractorDbConfig extractorDbConfig;

    @Value("${timescaledbHealthCheckIntervalSeconds:60}")
    private int timescaledbHealthCheckIntervalSeconds;

    /**
     * Retention period for the reporting metric table.
     *
     * <p>Todo: remove this once we support changing retention periods through our UI.</p>
     */
    @Value("${reportingMetricTableRetentionMonths:#{null}}")
    private Integer reportingMetricTableRetentionMonths;

    private void setupHealthMonitor() throws InterruptedException {
        logger.info("Adding PostgreSQL health checks to the component health monitor.");
        try {
            getHealthMonitor().addHealthCheck(new PostgreSQLHealthMonitor(
                    timescaledbHealthCheckIntervalSeconds,
                    extractorDbConfig.ingesterEndpoint().datasource()::getConnection));
        } catch (UnsupportedDialectException | SQLException e) {
            throw new IllegalStateException("DbEndpoint not available, could not start health monitor", e);
        }
    }

    @Override
    public void onDumpDiags(@Nonnull final ZipOutputStream diagnosticZip) {
        diagnosticsConfig.diagnosticsHandler().dump(diagnosticZip);
    }

    /**
     * Starts the component.
     *
     * @param args none expected
     */
    public static void main(String[] args) {
        startContext(ExtractorComponent.class);
    }

    @Override
    protected void onStartComponent() {
        logger.debug("Writer config: {}", listenerConfig.writerConfig());
        try {
            setupHealthMonitor();
            // change retention policy if custom period is provided
            if (reportingMetricTableRetentionMonths != null) {
                extractorDbConfig.ingesterEndpoint().getAdapter().setupRetentionPolicy(
                        "metric", ChronoUnit.MONTHS, reportingMetricTableRetentionMonths);
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            logger.error("Failed to set up health monitor -"
                + "interrupted while waiting for endpoint initialization", e);
        } catch (UnsupportedDialectException | SQLException e) {
            logger.error("Failed to create retention policy", e);
        }
    }
}
