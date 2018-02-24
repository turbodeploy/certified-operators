package com.vmturbo.kvstore;

import java.util.concurrent.TimeUnit;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/**
 * Configuration for the {@link KeyValueStore} used by components.
 */
@Configuration
public class KeyValueStoreConfig {
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

    protected String getConsulHost() {
        return consulHost;
    }

    protected String getConsulPort() {
        return consulPort;
    }

    protected String getApplicationName() {
        return applicationName;
    }

    protected long getKvStoreRetryIntervalMillis() {
        return kvStoreRetryIntervalMillis;
    }
}
