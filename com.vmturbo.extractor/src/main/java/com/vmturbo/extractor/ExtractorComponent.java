package com.vmturbo.extractor;

import java.sql.SQLException;
import java.time.temporal.ChronoUnit;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;

import com.google.common.util.concurrent.ThreadFactoryBuilder;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;

import com.vmturbo.components.common.BaseVmtComponent;
import com.vmturbo.components.common.health.sql.PostgreSQLHealthMonitor;
import com.vmturbo.extractor.grafana.GrafanaConfig;
import com.vmturbo.extractor.schema.ExtractorDbConfig;
import com.vmturbo.extractor.topology.TopologyListenerConfig;
import com.vmturbo.sql.utils.DbEndpoint.UnsupportedDialectException;
import com.vmturbo.sql.utils.SQLDatabaseConfig2;

/**
 * The Extractor component receiving information broadcast from various components in the system. It
 * writes data to timescaledb database for query, export, and search/sort/filter.
 */
@Configuration("theComponent")
@Import({TopologyListenerConfig.class,
        ExtractorDbConfig.class,
        SQLDatabaseConfig2.class,
        GrafanaConfig.class})
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
        // Initialize the database in a separate thread, so that we don't need to block while
        // waiting for the auth component (to get db password) to be available.
        ThreadFactory threadFactory = new ThreadFactoryBuilder()
                .setNameFormat("db-init")
                .setDaemon(true)
                .build();
        final ExecutorService executorService = Executors.newSingleThreadExecutor(threadFactory);
        executorService.submit((Callable<Void>)() -> {
            sqlDatabaseConfig.initAll();
            setupHealthMonitor();

            // change retention policy if custom period is provided
            if (reportingMetricTableRetentionMonths != null) {
                try {
                    extractorDbConfig.ingesterEndpoint().getAdapter().setupRetentionPolicy(
                            "metric", ChronoUnit.MONTHS, reportingMetricTableRetentionMonths);
                } catch (UnsupportedDialectException | SQLException e) {
                    logger.error("Failed to create retention policy", e);
                }
            }
            return null;
        });
        executorService.shutdown();
    }
}
