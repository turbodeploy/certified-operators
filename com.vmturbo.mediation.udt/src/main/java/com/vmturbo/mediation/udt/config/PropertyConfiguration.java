package com.vmturbo.mediation.udt.config;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.PropertySource;

/**
 * Configuration class used by Spring context.
 */
@Configuration
@PropertySource("classpath:udt.properties")
public class PropertyConfiguration {

    /**
     * Environment variable used by com.vmturbo.components.common.config.PropertiesLoader while
     * reading component`s properties.
     */
    @Value("${component_type:udt}")
    public String componentType;
}
