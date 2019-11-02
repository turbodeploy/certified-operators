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
import com.vmturbo.history.db.VmtDbException;
import com.vmturbo.history.diagnostics.HistoryDiagnosticsConfig;
import com.vmturbo.history.market.MarketListenerConfig;
import com.vmturbo.history.stats.StatsConfig;
import com.vmturbo.history.topology.TopologyListenerConfig;

/**
 * The main configuration for the History Component.
 */
@Configuration("theComponent")
@Import({HistoryDbConfig.class, TopologyListenerConfig.class, MarketListenerConfig.class,
    StatsConfig.class, HistoryApiConfig.class, ApiSecurityConfig.class, SpringSecurityConfig.class,
    HistoryDiagnosticsConfig.class,
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

    @Value("${mariadbHealthCheckIntervalSeconds:60}")
    private int mariaHealthCheckIntervalSeconds;

    @PostConstruct
    private void setup() {
        // perform the flyway migration to apply any database updates; errors -> failed spring init
        try {
            new HistoryDbMigration(historyDbConfig.historyDbIO())
                    .migrate();
        } catch (VmtDbException e) {
            throw new RuntimeException("DB Initialization / Migration error", e);
        }

        log.info("Adding MariaDB and Kafka producer health checks to the component health monitor.");
        getHealthMonitor().addHealthCheck(
                new MariaDBHealthMonitor(mariaHealthCheckIntervalSeconds,
                    historyDbConfig.historyDbIO()::connection));
        getHealthMonitor().addHealthCheck(historyApiConfig.kafkaProducerHealthMonitor());
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
        startContext(HistoryComponent.class);
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
