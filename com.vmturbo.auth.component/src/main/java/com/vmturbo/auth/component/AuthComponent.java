package com.vmturbo.auth.component;

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
import org.springframework.context.annotation.ComponentScan;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;

import io.grpc.Server;
import io.grpc.ServerBuilder;
import io.grpc.ServerInterceptors;
import me.dinowernli.grpc.prometheus.MonitoringServerInterceptor;

import com.vmturbo.auth.api.SpringSecurityConfig;
import com.vmturbo.auth.api.authorization.jwt.JwtServerInterceptor;
import com.vmturbo.auth.component.widgetset.WidgetsetConfig;
import com.vmturbo.components.common.BaseVmtComponent;
import com.vmturbo.components.common.BaseVmtComponentConfig;
import com.vmturbo.components.common.health.sql.MariaDBHealthMonitor;

/**
 * The main auth component.
 */
@Configuration("theComponent")
@EnableAutoConfiguration
@EnableDiscoveryClient
@ComponentScan({"com.vmturbo.auth.component.services", "com.vmturbo.auth.component.store"})
@Import({AuthRESTSecurityConfig.class, AuthDBConfig.class, SpringSecurityConfig.class,
        WidgetsetConfig.class})
public class AuthComponent extends BaseVmtComponent {
    /**
     * The logger.
     */
    private final Logger logger = LogManager.getLogger(AuthComponent.class);

    /**
     * The component name.
     */
    @Value("${spring.application.name}")
    private String componentName;

    @Value("${mariadbHealthCheckIntervalSeconds:60}")
    private int mariaHealthCheckIntervalSeconds;

    @Autowired
    private AuthDBConfig authDBConfig;

    @Autowired
    private AuthRESTSecurityConfig authRESTSecurityConfig;

    @Autowired
    private WidgetsetConfig widgetsetConfig;

    /**
     * JWT token verification and decoding.
     */
    @Autowired
    private SpringSecurityConfig securityConfig;

    @PostConstruct
    private void setup() {
        logger.info("Adding MariaDB health check to the component health monitor.");
        getHealthMonitor().addHealthCheck(
                new MariaDBHealthMonitor(mariaHealthCheckIntervalSeconds,
                        authDBConfig.dataSource()::getConnection));
    }

    /**
     * Starts the component.
     *
     * @param args The mandatory arguments.
     */
    public static void main(String[] args) {
        // apply the configuration properties for this component prior to Spring instantiation
        fetchConfigurationProperties();
        // instantiate and run this component
        new SpringApplicationBuilder()
                .sources(AuthComponent.class)
                .sources(BaseVmtComponentConfig.class)
                .run(args);
    }

    /**
     * Returns the component name.
     *
     * @return The component name.
     */
    @Override
    public String getComponentName() {
        return componentName;
    }

    @Override
    @Nonnull
    protected Optional<Server> buildGrpcServer(@Nonnull final ServerBuilder builder) {
        // Monitor for server metrics with prometheus.
        final MonitoringServerInterceptor monitoringInterceptor =
            MonitoringServerInterceptor.create(me.dinowernli.grpc.prometheus.Configuration.allMetrics());

        // gRPC JWT token interceptor
        final JwtServerInterceptor jwtInterceptor =
                new JwtServerInterceptor(securityConfig.apiAuthKVStore());
        return Optional.of(builder
                .addService(ServerInterceptors.intercept(widgetsetConfig.widgetsetRpcService(
                        authRESTSecurityConfig.targetStore()), jwtInterceptor, monitoringInterceptor))
                .build());
    }

}
