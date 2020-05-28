package com.vmturbo.integrations.intersight;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import com.vmturbo.mediation.connector.intersight.IntersightConnection;

/**
 * Spring configuration for an Intersight Connection
 */
@Configuration
public class IntersightConnectionConfig {

    @Value("${intersightHost:bordeaux.default}")
    private String intersightHost;

    @Value("${intersightPort:8443}")
    private int intersightPort;

    @Value("${intersightClientId}")
    private String intersightClientId;

    @Value("${intersightClientSecret}")
    private String intersightClientSecret;

    /**
     * Construct and return a {@link IntersightConnection} to access the Intersight instance.
     *
     * @return a {@link IntersightConnection} to access the Intersight instance
     */
    @Bean
    public IntersightConnection getIntersightConnection() {
        return new IntersightConnection(intersightHost, intersightPort, intersightClientId,
                intersightClientSecret);
    }

}
