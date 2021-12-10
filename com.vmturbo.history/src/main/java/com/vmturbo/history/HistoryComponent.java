package com.vmturbo.history;

import java.util.Collections;
import java.util.List;
import java.util.zip.ZipOutputStream;

import javax.annotation.Nonnull;
import javax.annotation.PostConstruct;

import io.grpc.BindableService;
import io.grpc.ServerInterceptor;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.jooq.exception.DataAccessException;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;

import com.vmturbo.auth.api.SpringSecurityConfig;
import com.vmturbo.auth.api.authorization.jwt.JwtServerInterceptor;
import com.vmturbo.components.common.BaseVmtComponent;
import com.vmturbo.components.common.health.sql.MariaDBHealthMonitor;
import com.vmturbo.history.api.ApiSecurityConfig;
import com.vmturbo.history.api.HistoryApiConfig;
import com.vmturbo.history.db.HistoryDbConfig;
import com.vmturbo.history.db.RetentionPolicy;
import com.vmturbo.history.db.bulk.BulkInserterFactory;
import com.vmturbo.history.diagnostics.HistoryDiagnosticsConfig;
import com.vmturbo.history.ingesters.IngestersConfig;
import com.vmturbo.history.stats.StatsConfig;
import com.vmturbo.sql.utils.dbmonitor.DbMonitor;

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
        HistoryDiagnosticsConfig.class
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
    private DbMonitor dbMonitorLoop;

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

    /**
     * Implicitly overrides default value for history component gRPC server parameter. This
     * parameter is using in com.vmturbo.components.api.grpc.ComponentGrpcServer#start(org.springframework.core.env.ConfigurableEnvironment).
     * By default most of the components are using significantly lower max message size. History
     * requires big max message size because of multiple gRPC functions which should pass big data,
     * e.g. percentile.
     */
    @Value("${grpcMaxMessageBytes:1048576000}")
    private int grpcMaxMessageBytes;

    @PostConstruct
    private void setup() {
        RetentionPolicy.init(historyDbConfig.dsl());
    }

    @Override
    public void onDumpDiags(@Nonnull final ZipOutputStream diagnosticZip) {
        diagnosticsConfig.historyDiagnostics().dump(diagnosticZip);
    }

    /**
     * This is the method that's called to initialize the component.
     *
     * @param args Command-line arguments.
     */
    public static void main(String[] args) {
        runComponent(HistoryComponent.class);
    }

    @Override
    protected void onStartComponent() {
        // drop any transient tables that would be orphaned by this shutdown
        if (historyDbConfig != null) {
            try {
                BulkInserterFactory.cleanupTransientTables(historyDbConfig.dsl());
            } catch (DataAccessException e) {
                log.warn("Failed to look for and clean up any orphaned transient tables", e);
            }
        }
        log.info("Starting topology coordinator");
        ingestersConfig.topologyCoordinator().startup();

        log.info(
                "Adding MariaDB and Kafka producer health checks to the component health monitor.");
        getHealthMonitor().addHealthCheck(
                new MariaDBHealthMonitor(mariaHealthCheckIntervalSeconds,
                        historyDbConfig.dataSource()::getConnection));
        getHealthMonitor().addHealthCheck(historyApiConfig.messageProducerHealthMonitor());

        if (historyDbConfig.isDbMonitorEnabled()) {
            historyDbConfig.startDbMonitor();
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
