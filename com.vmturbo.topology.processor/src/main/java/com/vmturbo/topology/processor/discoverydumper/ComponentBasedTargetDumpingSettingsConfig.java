package com.vmturbo.topology.processor.discoverydumper;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/**
 * Capture Spring configuration values for the {@link ComponentBasedTargetDumpingSettings}.
 */

@Configuration
public class ComponentBasedTargetDumpingSettingsConfig {
    @Value("${component_type}")
    private String componentType;

    @Value("${instance_id}")
    private String componentInstanceId;

    @Bean
    public ComponentBasedTargetDumpingSettings componentBasedTargetDumpingSettings() {
        return new ComponentBasedTargetDumpingSettings(componentType, componentInstanceId);
    }
}
