package com.vmturbo.topology.processor.consistentscaling;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class ConsistentScalingConfig {
    @Value("${consistentScalingEnabled:true }")
    private boolean enabled = true;

    @Bean
    public ConsistentScalingManager consistentScalingManager() {
        return new ConsistentScalingManager(this);
    }

    @Bean
    public ConsistentScalingConfig consistentScalingConfig() {
        return this;
    }

    public boolean isEnabled() {
        return enabled;
    }
}
