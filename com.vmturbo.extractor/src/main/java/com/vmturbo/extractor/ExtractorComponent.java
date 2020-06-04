package com.vmturbo.extractor;

import java.sql.SQLException;
import java.time.temporal.ChronoUnit;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;

import com.vmturbo.components.common.BaseVmtComponent;
import com.vmturbo.components.common.health.sql.PostgreSQLHealthMonitor;
import com.vmturbo.extractor.topology.TopologyListenerConfig;
import com.vmturbo.sql.utils.DbEndpoint.UnsupportedDialectException;
import com.vmturbo.sql.utils.SQLDatabaseConfig2;

/**
 * The Extractor component receiving information broadcast from various components in the system. It
 * writes data to timescaledb database for query, export, and search/sort/filter.
 */
@Configuration("theComponent")
@Import({TopologyListenerConfig.class, ExtractorDbConfig.class, SQLDatabaseConfig2.class})
public class ExtractorComponent extends BaseVmtComponent {
    private static final Logger logger = LogManager.getLogger();

    @Autowired
    private TopologyListenerConfig listenerConfig;

    @Autowired
    private ExtractorDbConfig extractorDbConfig;

    @Autowired
    private SQLDatabaseConfig2 sqlDatabaseConfig;

    @Value("${timescaledbHealthCheckIntervalSeconds:60}")
    private int timescaledbHealthCheckIntervalSeconds;

    /**
     * Retention period for the reporting metric table.
     * Todo: remove this once we support changing retention periods through our UI.
     */
    @Value("${reportingMetricTableRetentionMonths:#{null}}")
    private Integer reportingMetricTableRetentionMonths;

    private void setupHealthMonitor() {
        logger.info("Adding PostgreSQL health checks to the component health monitor.");
        try {
            getHealthMonitor().addHealthCheck(new PostgreSQLHealthMonitor(
                    timescaledbHealthCheckIntervalSeconds,
                    extractorDbConfig.ingesterEndpoint().get().datasource()::getConnection));
        } catch (InterruptedException | UnsupportedDialectException | SQLException e) {
            throw new IllegalStateException("DbEndpoint not available, could not start health monitor", e);
        }
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
            sqlDatabaseConfig.initAll();
        } catch (UnsupportedDialectException | SQLException | InterruptedException e) {
            throw new IllegalStateException("Failed to initialize data endpoints", e);
        }
        setupHealthMonitor();

        // change retention policy if custom period is provided
        if (reportingMetricTableRetentionMonths != null) {
            try {
                extractorDbConfig.ingesterEndpoint().get().getAdapter().setupRetentionPolicy(
                        "metric", ChronoUnit.MONTHS, reportingMetricTableRetentionMonths);
            } catch (UnsupportedDialectException | InterruptedException | SQLException e) {
                logger.error("Failed to create retention policy", e);
            }
        }
    }
}
