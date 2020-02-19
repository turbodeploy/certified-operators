package com.vmturbo.auth.component;

import java.util.concurrent.TimeUnit;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import com.vmturbo.auth.api.authorization.kvstore.AuthApiKVConfig;
import com.vmturbo.kvstore.ConsulKeyValueStore;
import com.vmturbo.kvstore.KeyValueStore;

/**
 * The key-value store config used in the Auth component. Auth stores several pieces of data
 * in consul, such as user accounts, licenses, database credentials and so on.
 */
@Configuration
public class AuthKVConfig {
    @Value("${consul_host:localhost}")
    private String consulHost;

    @Value("${consul_port:8500}")
    private String consulPort;

    @Value("${kvStoreTimeoutSeconds:120}")
    private long kvStoreTimeoutSeconds;

    @Bean
    public KeyValueStore authKeyValueStore() {
        return new ConsulKeyValueStore(AuthApiKVConfig.AUTH_NAMESPACE,
                consulHost,
                consulPort,
                kvStoreTimeoutSeconds,
                TimeUnit.SECONDS
        );
    }

}
