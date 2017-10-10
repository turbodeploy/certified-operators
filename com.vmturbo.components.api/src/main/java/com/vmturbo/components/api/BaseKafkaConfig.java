package com.vmturbo.components.api;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;

/**
 * Base configuration for Kafka, holding kafka severs initial list.
 */
public class BaseKafkaConfig  {

    /**
     * Bootstrap server - a list of kafka servers to try to connect to. All the server in the
     * cluster should be specified here.
     */
    @Value("${kafkaServers:none}")
    private String bootstrapServer;

    /**
     * Returns bootstrap servers list.
     * @return bootstrap servers list
     */
    @Bean
    protected String bootstrapServer() {
        return bootstrapServer;
    }
}
