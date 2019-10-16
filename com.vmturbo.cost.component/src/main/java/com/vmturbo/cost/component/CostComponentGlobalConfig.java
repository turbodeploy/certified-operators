package com.vmturbo.cost.component;

import java.time.Clock;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/**
 * Configuration for global beans shared across all configurations in the cost component.
 */
@Configuration
public class CostComponentGlobalConfig {

    /**
     * Shared clock to be used by all beans in the cost component.
     *
     * @return The {@link Clock}.
     */
    @Bean
    public Clock clock() {
        return Clock.systemUTC();
    }
}
