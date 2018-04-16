package com.vmturbo.components.common;

import org.springframework.cloud.client.serviceregistry.AutoServiceRegistrationProperties;
import org.springframework.cloud.commons.util.InetUtils;
import org.springframework.cloud.commons.util.InetUtilsProperties;
import org.springframework.cloud.consul.config.ConsulConfigBootstrapConfiguration;
import org.springframework.cloud.consul.serviceregistry.ConsulAutoServiceRegistrationAutoConfiguration;
import org.springframework.cloud.consul.serviceregistry.ConsulServiceRegistryAutoConfiguration;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;

/**
 * This configuration holds all the dependency configuration, required to run Consul client.
 */
@Configuration
@Import({ConsulConfigBootstrapConfiguration.class, ConsulServiceRegistryAutoConfiguration.class,
        AutoServiceRegistrationProperties.class,
        ConsulAutoServiceRegistrationAutoConfiguration.class})
public class ConsulDiscoveryManualConfig {

    @Bean
    protected InetUtils inetUtils() {
        return new InetUtils(new InetUtilsProperties());
    }
}
