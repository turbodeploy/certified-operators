package com.vmturbo.components.common;

import java.net.InetAddress;
import java.net.UnknownHostException;

import javax.annotation.PostConstruct;

import com.ecwid.consul.v1.ConsulClient;
import com.ecwid.consul.v1.ConsulRawClient;
import com.ecwid.consul.v1.agent.model.NewCheck;
import com.ecwid.consul.v1.agent.model.NewService;

import com.vmturbo.components.common.utils.EnvironmentUtils;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Configuration;

/**
 * This configuration holds all the dependency configuration, required to run Consul client.
 */
@Configuration
public class ConsulRegistrationConfig {

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

    private Boolean enableConsulRegistration;

    @PostConstruct
    protected void registerConsul() throws UnknownHostException {
        enableConsulRegistration = EnvironmentUtils.getOptionalEnvProperty(ConsulDiscoveryManualConfig.ENABLE_CONSUL_REGISTRATION)
                .map(Boolean::parseBoolean)
                .orElse(false);

        if (!enableConsulRegistration) {
            return;
        }
        final ConsulRawClient rawClient = new ConsulRawClient(consulHost, consulPort);
        final ConsulClient client = new ConsulClient(rawClient);
        final NewService svc = new NewService();
        svc.setName(componentType);
        svc.setId(instanceId);
        svc.setAddress(InetAddress.getLocalHost().getHostName());
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
