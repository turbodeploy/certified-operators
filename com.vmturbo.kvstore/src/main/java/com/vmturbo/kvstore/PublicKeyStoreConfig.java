package com.vmturbo.kvstore;


import java.util.concurrent.TimeUnit;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/**
 * Configuration for the {@link KeyValueStore} used by the XL components.
 * It's currently only used in API component.
 */
@Configuration
public class PublicKeyStoreConfig extends KeyValueStoreConfig {

    @Value("${consulNamespace:}")
    private String consulNamespace;

    @Value("${enableConsulNamespace:false}")
    private boolean enableConsulNamespace;

    @Bean
    public IPublicKeyStore publicKeyStore() {
        return new PublicKeyStore(
                ConsulKeyValueStore.constructNamespacePrefix(consulNamespace, enableConsulNamespace),
                getApplicationName(),
                getConsulHost(),
                getConsulPort(),
                getKvStoreTimeoutSecond(),
                TimeUnit.SECONDS
        );
    }
}
