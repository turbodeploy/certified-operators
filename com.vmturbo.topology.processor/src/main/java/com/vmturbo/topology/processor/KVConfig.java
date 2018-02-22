package com.vmturbo.topology.processor;

import java.util.concurrent.TimeUnit;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import com.vmturbo.kvstore.ConsulKeyValueStore;
import com.vmturbo.kvstore.KeyValueStore;

/**
 * Configuration for the {@link KeyValueStore} used by the
 * topology processor.
 */
@Configuration
public class KVConfig {
    @Value("${spring.cloud.consul.host}")
    private String consulHost;

    @Value("${spring.cloud.consul.port}")
    private String consulPort;

    @Value("${spring.application.name}")
    private String applicationName;

    @Value("${kvStoreRetryIntervalMillis}")
    private long kvStoreRetryIntervalMillis;

    @Bean
    public KeyValueStore keyValueStore() {
        return new ConsulKeyValueStore(
            applicationName,
            consulHost,
            consulPort,
            kvStoreRetryIntervalMillis,
            TimeUnit.MILLISECONDS
        );
    }

    @Bean
    public KeyValueStore keyValueStoreTelemetry() {
        return new ConsulKeyValueStore(
                "telemetry",
                consulHost,
                consulPort,
                kvStoreRetryIntervalMillis,
                TimeUnit.MILLISECONDS
        );
    }
}
