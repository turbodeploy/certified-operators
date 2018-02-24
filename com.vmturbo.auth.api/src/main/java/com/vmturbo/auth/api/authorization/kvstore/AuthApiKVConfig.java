package com.vmturbo.auth.api.authorization.kvstore;

import java.util.concurrent.TimeUnit;

import com.vmturbo.kvstore.ConsulKeyValueStore;
import com.vmturbo.kvstore.KeyValueStore;

import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/**
 * Configuration for the {@link KeyValueStore} used by the
 * topology processor.
 */
@Configuration
public class AuthApiKVConfig {
    @Value("${spring.cloud.consul.host}")
    private String consulHost;

    @Value("${spring.cloud.consul.port}")
    private String consulPort;

    @Value("${spring.application.name}")
    private String applicationName;

    @Value("${kvStoreRetryIntervalMillis}")
    private long kvStoreRetryIntervalMillis;

    /**
     * Construct the key/value store that is attached to auth component.
     *
     * @return The key/value store bean.
     */
    @Qualifier("authKeyValueStore")
    @Bean
    public KeyValueStore authKeyValueStore() {
        return new ConsulKeyValueStore(
            "auth",
            consulHost,
            consulPort,
            kvStoreRetryIntervalMillis,
            TimeUnit.MILLISECONDS
        );
    }
}
