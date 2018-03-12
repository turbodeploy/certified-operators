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
    private HistoryApiConfig historyApiConfig;

    @Autowired
    private StatsConfig statsConfig;

    @Value("${mariadbHealthCheckIntervalSeconds:60}")
    private int mariaHealthCheckIntervalSeconds;

    @PostConstruct
    private void setup() {
        log.info("Adding MariaDB and Kafka producer health checks to the component health monitor.");
        getHealthMonitor().addHealthCheck(
                new MariaDBHealthMonitor(mariaHealthCheckIntervalSeconds,historyDbConfig.historyDbIO()::connection));
        getHealthMonitor().addHealthCheck(historyApiConfig.kafkaProducerHealthMonitor());
    }

    public static void main(String[] args) {
        // apply the configuration properties for this component prior to Spring instantiation
        fetchConfigurationProperties();
        // instantiate and run this component
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
