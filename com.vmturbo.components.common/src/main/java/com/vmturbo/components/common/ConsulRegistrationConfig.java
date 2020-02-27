package com.vmturbo.components.common;

import java.time.Clock;

import javax.annotation.PostConstruct;

import com.ecwid.consul.v1.ConsulClient;
import com.ecwid.consul.v1.ConsulRawClient;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import com.vmturbo.components.common.diagnostics.DiagnosticService;
import com.vmturbo.components.common.health.ConsulHealthcheckRegistration;
import com.vmturbo.kvstore.ConsulKeyValueStore;

/**
 * This configuration holds all the dependency configuration, required to run Consul client.
 */
@Configuration
public class ConsulRegistrationConfig {

    private static final Logger logger = LogManager.getLogger(DiagnosticService.class);

    /**
     * Key for environment variable to control:  if "true" this component should register
     * as a Service with Consul since it is running in the main cluster; if "false" then
     * the component should **not** register with Consul as a service.
     */
    public static final String ENABLE_CONSUL_REGISTRATION = "enableConsulRegistration";

    @Value("${consul_host}")
    private String consulHost;
    @Value("${consul_port}")
    private Integer consulPort;

    @Value("${consulMaxRetrySecs:60}")
    private int consulMaxRetrySecs;

    @Value("${" + BaseVmtComponent.PROP_serverHttpPort + '}')
    private Integer serverPort;

    @Value("${" + BaseVmtComponent.PROP_COMPONENT_TYPE + '}')
    private String componentType;
    @Value("${" + BaseVmtComponent.PROP_INSTANCE_ID + '}')
    private String instanceId;
    @Value("${" + BaseVmtComponent.PROP_INSTANCE_IP + ":}")
    private String instanceIp;
    @Value("${" + BaseVmtComponent.PROP_INSTANCE_ROUTE + ":}")
    private String instanceRoute;
    @Value("${consulNamespace:}")
    private String consulNamespace;
    @Value("${enableConsulNamespace:false}")
    private boolean enableConsulNamespace;

    /**
     * This property is used to disable consul registration. This is necessary for tests and
     * for components running outside the primary Turbonomic K8s cluster.
     */
    @Value("${" + ENABLE_CONSUL_REGISTRATION + ":true}")
    private Boolean enableConsulRegistration;


    /**
     * Create a handler for Consul service registration to track Component status.
     *
     * @return a new instance of ConsulHealthcheckRegistration to handle Consul Service registration
     */
    @Bean
    public ConsulHealthcheckRegistration consulHealthcheckRegistration() {
        final ConsulRawClient rawClient = new ConsulRawClient(consulHost, consulPort);
        final ConsulClient consulClient = new ConsulClient(rawClient);
        return new ConsulHealthcheckRegistration(consulClient, enableConsulRegistration,
            componentType, instanceId, instanceIp, instanceRoute, serverPort, consulMaxRetrySecs,
            ConsulKeyValueStore.constructNamespacePrefix(consulNamespace, enableConsulNamespace),
            Clock.systemUTC());
    }

    /**
     * Trigger Consul registration, if enabled, after Spring Instantiating is complete.
     */
    @PostConstruct
    protected void registerConsul() {
        if (enableConsulRegistration) {
            consulHealthcheckRegistration().registerService();
        } else {
            logger.info("Consul Registration Disabled.");
        }

    }
}
