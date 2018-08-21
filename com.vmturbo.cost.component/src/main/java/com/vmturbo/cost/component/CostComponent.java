package com.vmturbo.cost.component;

import java.util.Optional;

import javax.annotation.Nonnull;
import javax.annotation.PostConstruct;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;

import io.grpc.Server;
import io.grpc.ServerBuilder;

import com.vmturbo.auth.api.SpringSecurityConfig;
import com.vmturbo.auth.api.authorization.jwt.JwtServerInterceptor;
import com.vmturbo.components.common.BaseVmtComponent;
import com.vmturbo.components.common.health.sql.MariaDBHealthMonitor;
import com.vmturbo.cost.component.topology.TopologyListenerConfig;
import com.vmturbo.sql.utils.SQLDatabaseConfig;

/**
 * The main cost component.
 */
@Configuration("theComponent")
@Import({TopologyListenerConfig.class,
        SpringSecurityConfig.class,
        SQLDatabaseConfig.class})
public class CostComponent extends BaseVmtComponent {
    /**
     * The logger.
     */
    private final Logger logger = LogManager.getLogger();

    @Autowired
    private SQLDatabaseConfig dbConfig;

    @Value("${mariadbHealthCheckIntervalSeconds:60}")
    private int mariaHealthCheckIntervalSeconds;

    /**
     * JWT token verification and decoding.
     */
    @Autowired
    private SpringSecurityConfig securityConfig;

    /**
     * Starts the component.
     *
     * @param args The mandatory arguments.
     */
    public static void main(String[] args) {
        startContext(CostComponent.class);
    }

    @PostConstruct
    private void setup() {
        logger.info("Adding MariaDB health check to the component health monitor.");
        getHealthMonitor()
                .addHealthCheck(new MariaDBHealthMonitor(mariaHealthCheckIntervalSeconds,
                        dbConfig.dataSource()::getConnection));
    }

    @Override
    @Nonnull
    protected Optional<Server> buildGrpcServer(@Nonnull final ServerBuilder builder) {
        // gRPC JWT token interceptor
        final JwtServerInterceptor jwtInterceptor =
                new JwtServerInterceptor(securityConfig.apiAuthKVStore());
        return Optional.of(builder.build());
    }
}
