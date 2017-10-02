package com.vmturbo.auth.component;

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

import com.vmturbo.auth.api.SpringSecurityConfig;
import com.vmturbo.components.common.BaseVmtComponent;
import com.vmturbo.components.common.BaseVmtComponentConfig;
import com.vmturbo.components.common.health.sql.SQLDBHealthMonitor;

/**
 * The main auth component.
 */
@Configuration("theComponent")
@EnableAutoConfiguration
@EnableDiscoveryClient
@ComponentScan({"com.vmturbo.auth.component.services", "com.vmturbo.auth.component.store"})
@Import({AuthRESTSecurityConfig.class, AuthDBConfig.class, SpringSecurityConfig.class})
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

    @PostConstruct
    private void setup() {
        logger.info("Adding MariaDB health check to the component health monitor.");
        getHealthMonitor().addHealthCheck("MariaDB",
                new SQLDBHealthMonitor(mariaHealthCheckIntervalSeconds,authDBConfig.dataSource()::getConnection));
    }

    /**
     * Starts the component.
     *
     * @param args The mandatory arguments.
     */
    public static void main(String[] args) {
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
}
