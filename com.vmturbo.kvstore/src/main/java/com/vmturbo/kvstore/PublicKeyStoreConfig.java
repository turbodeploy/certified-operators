package com.vmturbo.kvstore;


import java.util.concurrent.TimeUnit;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/**
 * Configuration for the {@link KeyValueStore} used by the XL components.
 * It's currently only used in API component.
 */
@Configuration
public class PublicKeyStoreConfig extends KeyValueStoreConfig {

    @Bean
    public IPublicKeyStore publicKeyStore() {
        return new PublicKeyStore(
                getApplicationName(),
                getConsulHost(),
                getConsulPort(),
                getKvStoreTimeoutSecond(),
                TimeUnit.SECONDS
        );
    }
}
