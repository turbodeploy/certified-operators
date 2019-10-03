package com.vmturbo.components.common;

import java.net.InetAddress;
import java.net.UnknownHostException;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;

import com.ecwid.consul.v1.ConsulClient;
import com.ecwid.consul.v1.ConsulRawClient;
import com.ecwid.consul.v1.agent.model.NewCheck;
import com.ecwid.consul.v1.agent.model.NewService;

import org.apache.commons.lang.StringUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Configuration;

import com.vmturbo.components.common.utils.EnvironmentUtils;

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
    @Value("${" + BaseVmtComponent.PROP_serverHttpPort + '}')
    private Integer serverPort;

    @Value("${" + BaseVmtComponent.PROP_COMPONENT_TYPE + '}')
    private String componentType;
    @Value("${" + BaseVmtComponent.PROP_INSTANCE_ID + '}')
    private String instanceId;
    @Value("${" + BaseVmtComponent.PROP_INSTANCE_IP + ":}")
    private String instanceIp;

    private Boolean enableConsulRegistration;

    /**
     * On Spring startup, if ENABLE_CONSUL_REGISTRATION is set "true" or empty,
     * register this component as a service with Consul using the
     * component-type as the service name, the unique instance-id as the service id,
     * and the IP address of the component as the service address.
     *
     *<p>Also register a health check with Consul that calls the /health endpoint
     * in the component.
     *
     * @throws UnknownHostException if cannot get the hostname for the current container
     */
    @PostConstruct
    protected void registerConsul() throws UnknownHostException {
        enableConsulRegistration = EnvironmentUtils
            .getOptionalEnvProperty(ENABLE_CONSUL_REGISTRATION)
            .map(Boolean::parseBoolean)
            .orElse(false);

        if (!enableConsulRegistration) {
            logger.info("Consul Registration Disabled.");
        } else {
            logger.info("Registering with Consul.");
            final ConsulRawClient rawClient = new ConsulRawClient(consulHost, consulPort);
            final ConsulClient client = new ConsulClient(rawClient);
            final NewService svc = new NewService();
            svc.setName(componentType);
            svc.setId(instanceId);
            // for backwards compatibility with docker-compose invocation, look up instanceIp
            if (StringUtils.isEmpty(instanceIp)) {
                instanceIp = InetAddress.getLocalHost().getHostAddress();
            }
            svc.setAddress(instanceIp);
            svc.setPort(serverPort);
            client.agentServiceRegister(svc);
            final NewCheck healthCheck = new NewCheck();
            healthCheck.setId("service:" + svc.getId());
            healthCheck.setName("Service '" + svc.getName() + "' check");
            healthCheck.setHttp("http://" + svc.getAddress() + ':' + svc.getPort() +
                ComponentController.HEALTH_PATH);
            healthCheck.setServiceId(svc.getId());
            healthCheck.setInterval("60s");
            client.agentCheckRegister(healthCheck);
        }
    }

    /**
     * On exit from Spring, deregister this service instance from Consul if the
     * ENABLE_CONSUL_REGISTRATION property is true.
     */
    @PreDestroy
    protected void deregisterService() {
        logger.info("should we De-Register with Consul?");

        enableConsulRegistration = EnvironmentUtils.getOptionalEnvProperty(ENABLE_CONSUL_REGISTRATION)
            .map(Boolean::parseBoolean)
            .orElse(false);
        if (!enableConsulRegistration) {
            return;
        }
        logger.info("De-Registering with Consul.");
        final ConsulRawClient rawClient = new ConsulRawClient(consulHost, consulPort);
        final ConsulClient client = new ConsulClient(rawClient);
        client.agentServiceDeregister(instanceId);
    }
}
