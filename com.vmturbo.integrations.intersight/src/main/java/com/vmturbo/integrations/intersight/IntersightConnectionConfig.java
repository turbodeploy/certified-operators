package com.vmturbo.integrations.intersight;

import java.time.Duration;
import java.util.Optional;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import com.vmturbo.mediation.connector.intersight.IntersightConnection;

/**
 * Spring configuration for an Intersight Connection
 */
@Configuration
public class IntersightConnectionConfig {
    private static final Logger logger = LogManager.getLogger();

    @Value("${intersightHost:http://bullwinkle.default.svc.cluster.local}")
    private String intersightHost;

    @Value("${intersightPort:7979}")
    private int intersightPort;

    @Value("${intersightClientId:890ba3015647ba92fa922b62d668375972da829970eb0004f1854dbae9f17089}")
    private String intersightClientId;

    @Value("${intersightClientSecret:cf21a100e648b5120d98061fc306a297}")
    private String intersightClientSecret;

    // request a new token when the current token expiration time goes below this threshold. Set to
    // 0 to disable.
    @Value("${intersightTokenRefreshThresholdSeconds:60}")
    private int intersightTokenRefreshThresholdSecs;

    /**
     * Construct and return a {@link IntersightConnection} to access the Intersight instance.
     *
     * @return a {@link IntersightConnection} to access the Intersight instance
     */
    @Bean
    public IntersightConnection getIntersightConnection() {
        if (intersightTokenRefreshThresholdSecs > 0) {
            return new IntersightConnection(intersightHost, intersightPort, intersightClientId,
                    intersightClientSecret, Optional.of(Duration.ofSeconds(intersightTokenRefreshThresholdSecs)));
        } else {
            logger.info("Creating Intersight Connection ");
            return new IntersightConnection(intersightHost, intersightPort, intersightClientId,
                    intersightClientSecret);
        }
    }

}
