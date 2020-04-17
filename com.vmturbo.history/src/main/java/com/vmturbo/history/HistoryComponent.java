package com.vmturbo.history;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.ThreadFactory;
import java.util.zip.ZipOutputStream;

import javax.annotation.Nonnull;
import javax.annotation.PostConstruct;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.google.common.util.concurrent.ThreadFactoryBuilder;

import io.grpc.BindableService;
import io.grpc.ServerInterceptor;

import org.apache.commons.lang3.StringUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.jooq.impl.DSL;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;

import com.vmturbo.auth.api.SpringSecurityConfig;
import com.vmturbo.auth.api.authorization.jwt.JwtServerInterceptor;
import com.vmturbo.components.common.BaseVmtComponent;
import com.vmturbo.components.common.health.sql.MariaDBHealthMonitor;
import com.vmturbo.history.api.ApiSecurityConfig;
import com.vmturbo.history.api.HistoryApiConfig;
import com.vmturbo.history.db.DBConnectionPool;
import com.vmturbo.history.db.HistoryDbConfig;
import com.vmturbo.history.db.VmtDbException;
import com.vmturbo.history.db.bulk.BulkInserterFactory;
import com.vmturbo.history.dbmonitor.DbMonitorConfig;
import com.vmturbo.history.diagnostics.HistoryDiagnosticsConfig;
import com.vmturbo.history.ingesters.IngestersConfig;
import com.vmturbo.history.stats.StatsConfig;

/**
 * Spring configuration for history component.
 */
@Configuration("theComponent")
@Import({
        HistoryDbConfig.class,
        IngestersConfig.class,
        StatsConfig.class,
        HistoryApiConfig.class,
        ApiSecurityConfig.class,
        SpringSecurityConfig.class,
        HistoryDiagnosticsConfig.class,
        DbMonitorConfig.class
})
public class HistoryComponent extends BaseVmtComponent {

    private static final Logger log = LogManager.getLogger();

    @Autowired
    private HistoryDbConfig historyDbConfig;

    @Autowired
    private HistoryApiConfig historyApiConfig;

    @Autowired
    private StatsConfig statsConfig;

    @Autowired
    private SpringSecurityConfig springSecurityConfig;

    @Autowired
    private HistoryDiagnosticsConfig diagnosticsConfig;

    @Autowired
    private DbMonitorConfig dbMonitorConfig;

    /**
     * This gives us access to the TopologyCoordinator instance, which manages ingestion and
     * rollup activities related to topologies received by history component.
     */
    @Autowired
    private IngestersConfig ingestersConfig;

    @Value("${mariadbHealthCheckIntervalSeconds:60}")
    private int mariaHealthCheckIntervalSeconds;

    @Value("${migrationLocation:}")
    private String migrationLocation;

    @PostConstruct
    private void setup() {
    }

    @Override
    public void onDumpDiags(@Nonnull final ZipOutputStream diagnosticZip) {
        diagnosticsConfig.historyDiagnostics().dump(diagnosticZip);
    }

    /**
     * The history utility that performs database migrations.
     *
     * @return The {@link HistoryDbMigration}.
     */
    @Bean
    public HistoryDbMigration dbMigration() {
        return new HistoryDbMigration(historyDbConfig.historyDbIO(),
            StringUtils.isEmpty(migrationLocation) ? Optional.empty() : Optional.of(migrationLocation));
    }

    /**
     * This is the method that's called to initialize the component.
     *
     * @param args Command-line arguments.
     */
    public static void main(String[] args) {
        startContext(HistoryComponent.class);
    }

    @Override
    protected void onStartComponent() {
        // perform the flyway migration to apply any database updates; errors -> failed spring init
        try {
            dbMigration().migrate();
        } catch (VmtDbException e) {
            throw new RuntimeException("DB Initialization / Migration error", e);
        }
        // drop any transient tables that would be orphaned by this shutdown
        if (DBConnectionPool.instance != null) {
            try (Connection conn = DBConnectionPool.instance.getConnection()) {
                BulkInserterFactory.cleanupTransientTables(DSL.using(conn));
            } catch (SQLException | VmtDbException e) {
                log.warn("Failed to look for and clean up any orphaned transient tables", e);
            }
        }
        log.info("Starting topology coordinator");
        ingestersConfig.topologyCoordinator().startup();

        log.info("Adding MariaDB and Kafka producer health checks to the component health monitor.");
        getHealthMonitor().addHealthCheck(
                new MariaDBHealthMonitor(mariaHealthCheckIntervalSeconds, historyDbConfig.historyDbIO()::connection));
        getHealthMonitor().addHealthCheck(historyApiConfig.kafkaProducerHealthMonitor());
        if (dbMonitorConfig.isEnabled()) {
            log.info("Starting Database monitor");
            startDbMonitor();
        }
    }

    @Override
    protected void onStopComponent() {
        super.onStopComponent();
        // Release all pooled DB connections (including actively borrowed) and shut down the pool
        // We have no way to close unpooled connections
        if (DBConnectionPool.instance != null) {
            log.info("Shutting down connection pool");
            if (DBConnectionPool.instance.getInternalPool() != null) {
                DBConnectionPool.instance.getInternalPool().close(true);
            }
            DBConnectionPool.instance.shutdown();
        }
    }

    private void startDbMonitor() {
        final ThreadFactory threadFactory = new ThreadFactoryBuilder()
                .setNameFormat("db-monitor-%d")
                .setDaemon(true)
                .build();
        threadFactory.newThread(() -> runDbMonitor()).start();
    }

    private void runDbMonitor() {
        try {
            dbMonitorConfig.dbMonitorLoop().run();
        } catch (InterruptedException e) {
            log.error("Monitoring interrupted; db monitoring suspended");
        } catch (JsonProcessingException e) {
            log.error("Malformed processListClassification value; db monitoring disabled", e.getMessage());
        }
    }

    @Nonnull
    @Override
    public List<BindableService> getGrpcServices() {
        return Collections.singletonList(statsConfig.statsRpcService());
    }

    @Nonnull
    @Override
    public List<ServerInterceptor> getServerInterceptors() {
        return Collections.singletonList(
            new JwtServerInterceptor(springSecurityConfig.apiAuthKVStore()));
    }
}
