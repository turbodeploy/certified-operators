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
    @Value("${consul_host}")
    private String consulHost;

    @Value("${consul_port}")
    private String consulPort;

    @Value("${instance_id}")
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
