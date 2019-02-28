package com.vmturbo.history;

import java.util.Optional;

import javax.annotation.Nonnull;
import javax.annotation.PostConstruct;

import io.grpc.Server;
import io.grpc.ServerBuilder;
import io.grpc.ServerInterceptors;

import me.dinowernli.grpc.prometheus.MonitoringServerInterceptor;

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
import com.vmturbo.history.market.MarketListenerConfig;
import com.vmturbo.history.stats.StatsConfig;
import com.vmturbo.history.topology.TopologyListenerConfig;

@Configuration("theComponent")
@Import({HistoryDbConfig.class, TopologyListenerConfig.class, MarketListenerConfig.class,
        StatsConfig.class, HistoryApiConfig.class, ApiSecurityConfig.class, SpringSecurityConfig.class,
})
public class HistoryComponent extends BaseVmtComponent {

    private final static Logger log = LogManager.getLogger();

    @Autowired
    private HistoryDbConfig historyDbConfig;

    @Autowired
    private HistoryApiConfig historyApiConfig;

    @Autowired
    private StatsConfig statsConfig;

    @Autowired
    private SpringSecurityConfig springSecurityConfig;

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
                new MariaDBHealthMonitor(mariaHealthCheckIntervalSeconds,historyDbConfig.historyDbIO()::connection));
        getHealthMonitor().addHealthCheck(historyApiConfig.kafkaProducerHealthMonitor());
    }

    public static void main(String[] args) {
        startContext(HistoryComponent.class);
    }

    @Override
    @Nonnull
    protected Optional<Server> buildGrpcServer(@Nonnull final ServerBuilder builder) {
        // Monitor for server metrics with prometheus.
        final MonitoringServerInterceptor monitoringInterceptor =
            MonitoringServerInterceptor.create(me.dinowernli.grpc.prometheus.Configuration.allMetrics());

        // gRPC JWT token interceptor
        final JwtServerInterceptor jwtInterceptor = new JwtServerInterceptor(springSecurityConfig.apiAuthKVStore());

        return Optional.of(builder
            .addService(ServerInterceptors.intercept(statsConfig.statsRpcService(), jwtInterceptor, monitoringInterceptor))
            .build());
    }
}
