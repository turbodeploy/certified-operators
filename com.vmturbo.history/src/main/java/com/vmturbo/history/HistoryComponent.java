package com.vmturbo.history;

import java.sql.SQLException;
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
import org.springframework.beans.factory.BeanCreationException;
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
import com.vmturbo.history.db.DbAccessConfig;
import com.vmturbo.history.db.RetentionPolicy;
import com.vmturbo.history.db.bulk.BulkInserterFactory;
import com.vmturbo.history.db.procedure.StoredProcedureConfig;
import com.vmturbo.history.diagnostics.HistoryDiagnosticsConfig;
import com.vmturbo.history.ingesters.IngestersConfig;
import com.vmturbo.history.stats.StatsConfig;
import com.vmturbo.sql.utils.DbEndpoint.UnsupportedDialectException;

/**
 * Spring configuration for history component.
 */
@Configuration("theComponent")
@Import({
        DbAccessConfig.class,
        IngestersConfig.class,
        StatsConfig.class,
        HistoryApiConfig.class,
        ApiSecurityConfig.class,
        SpringSecurityConfig.class,
        HistoryDiagnosticsConfig.class,
        StoredProcedureConfig.class
})
public class HistoryComponent extends BaseVmtComponent {

    private static final Logger log = LogManager.getLogger();

    @Autowired
    private DbAccessConfig dbAccessConfig;

    @Autowired
    private HistoryApiConfig historyApiConfig;

    @Autowired
    private StatsConfig statsConfig;

    @Autowired
    private SpringSecurityConfig springSecurityConfig;

    @Autowired
    private HistoryDiagnosticsConfig diagnosticsConfig;

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
        try {
            RetentionPolicy.init(dbAccessConfig.dsl());
        } catch (SQLException | UnsupportedDialectException | InterruptedException e) {
            if (e instanceof InterruptedException) {
                Thread.currentThread().interrupt();
            }
            throw new BeanCreationException("Failed to initialize RetentionPolicy", e);
        }
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
        if (dbAccessConfig != null) {
            try {
                BulkInserterFactory.cleanupTransientTables(dbAccessConfig.dsl());
            } catch (DataAccessException | SQLException | UnsupportedDialectException | InterruptedException e) {
                log.warn("Failed to look for and clean up any orphaned transient tables", e);
                if (e instanceof InterruptedException) {
                    Thread.currentThread().interrupt();
                }
            }
        }
        log.info("Starting topology coordinator");
        ingestersConfig.topologyCoordinator().startup();

        log.info("Adding DB and Kafka producer health checks to the component health monitor.");
        try {
            getHealthMonitor().addHealthCheck(
                    new MariaDBHealthMonitor(mariaHealthCheckIntervalSeconds,
                            dbAccessConfig.dataSource()::getConnection));
            getHealthMonitor().addHealthCheck(historyApiConfig.messageProducerHealthMonitor());
        } catch (InterruptedException | SQLException | UnsupportedDialectException e) {
            if (e instanceof InterruptedException) {
                Thread.currentThread().interrupt();
            }
            throw new BeanCreationException("Failed to start DB health monitor", e);
        }
        if (dbAccessConfig.isDbMonitorEnabled()) {
            dbAccessConfig.startDbMonitor();
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
