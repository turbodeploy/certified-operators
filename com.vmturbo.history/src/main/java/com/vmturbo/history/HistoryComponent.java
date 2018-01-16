package com.vmturbo.history;

import java.util.Optional;

import javax.annotation.Nonnull;
import javax.annotation.PostConstruct;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.boot.builder.SpringApplicationBuilder;
import org.springframework.cloud.client.discovery.EnableDiscoveryClient;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;
import org.springframework.web.socket.server.standard.ServerEndpointExporter;

import io.grpc.Server;
import io.grpc.ServerBuilder;

import com.vmturbo.auth.api.db.DBPasswordUtil;
import com.vmturbo.components.common.BaseVmtComponent;
import com.vmturbo.history.api.HistoryApiConfig;
import com.vmturbo.history.api.ApiSecurityConfig;
import com.vmturbo.history.db.HistoryDbConfig;
import com.vmturbo.history.db.HistorydbIO;
import com.vmturbo.history.market.MarketListenerConfig;
import com.vmturbo.history.stats.StatsConfig;
import com.vmturbo.history.topology.TopologyListenerConfig;
import com.vmturbo.reports.db.VmtDbException;
import com.vmturbo.components.common.health.sql.SQLDBHealthMonitor;

@Configuration("theComponent")
@EnableAutoConfiguration
@EnableDiscoveryClient
@Import({HistoryDbConfig.class, TopologyListenerConfig.class, MarketListenerConfig.class,
        StatsConfig.class, HistoryApiConfig.class, ApiSecurityConfig.class})
public class HistoryComponent extends BaseVmtComponent {

    private final static Logger log = LogManager.getLogger();

    @Value("${spring.application.name}")
    private String componentName;

    @Autowired
    private HistoryDbConfig historyDbConfig;

    @Autowired
    private StatsConfig statsConfig;

    @Value("${mariadbHealthCheckIntervalSeconds:60}")
    private int mariaHealthCheckIntervalSeconds;

    @PostConstruct
    private void setup() {
        log.info("Adding MariaDB health check to the component health monitor.");
        getHealthMonitor().addHealthCheck("MariaDB",
                new SQLDBHealthMonitor(mariaHealthCheckIntervalSeconds,
                        historyDbConfig.historyDbIO()::connection));
    }

    public static void main(String[] args) {
        new SpringApplicationBuilder()
                .sources(HistoryComponent.class)
                .run(args);
    }

    @Override
    public String getComponentName() {
        return componentName;
    }

    /**
     * Begin execution of the History Component.
     *
     * The main task is to perform database migration. If that task fails, the component is transitioned to
     * the FAILED state.
     */
    @Override
    public void onStartComponent() {
        try {
            new HistoryDbMigration(historyDbConfig.historyDbIO())
                    .migrate();
        } catch (VmtDbException e) {
            log.error("DB Initialization error", e);
            failedComponent();
            return;
        }
        super.onStartComponent();
    }

    @Override
    @Nonnull
    protected Optional<Server> buildGrpcServer(@Nonnull final ServerBuilder builder) {
        return Optional.of(builder
                .addService(statsConfig.statsRpcService())
                .build());
    }

    /**
     * This bean performs registration of all configured websocket endpoints.
     *
     * @return bean
     */
    @Bean
    public ServerEndpointExporter endpointExporter() {
        return new ServerEndpointExporter();
    }
}
