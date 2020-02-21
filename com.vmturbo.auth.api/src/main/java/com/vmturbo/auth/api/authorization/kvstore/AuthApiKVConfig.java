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

    public static final String AUTH_NAMESPACE = "auth";

    @Value("${consul_host}")
    private String consulHost;

    @Value("${consul_port}")
    private String consulPort;

    @Value("${instance_id}")
    private String applicationName;

    @Value("${kvStoreTimeoutSeconds:120}")
    private long kvStoreTimeoutSeconds;

    @Value("${consulNamespace:}")
    private String consulNamespace;

    @Value("${enableConsulNamespace:false}")
    private boolean enableConsulNamespace;

    /**
     * Construct the key/value store that is attached to auth component.
     *
     * @return The key/value store bean.
     */
    @Qualifier("authKeyValueStore")
    @Bean
    public KeyValueStore authKeyValueStore() {
        return new ConsulKeyValueStore(
            ConsulKeyValueStore.constructNamespacePrefix(consulNamespace, enableConsulNamespace),
            AUTH_NAMESPACE,
            consulHost,
            consulPort,
            kvStoreTimeoutSeconds,
            TimeUnit.SECONDS
        );
    }
}
