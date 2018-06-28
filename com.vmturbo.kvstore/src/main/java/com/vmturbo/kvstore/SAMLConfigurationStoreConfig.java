package com.vmturbo.kvstore;


import java.util.concurrent.TimeUnit;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/**
 * Configuration for the {@link SAMLConfigurationStore} used by the API components.
 */
@Configuration
public class SAMLConfigurationStoreConfig extends KeyValueStoreConfig {

    @Bean
    public ISAMLConfigurationStore samlConfigurationStore() {
        return new SAMLConfigurationStore(
                getApplicationName(),
                getConsulHost(),
                getConsulPort(),
                getKvStoreRetryIntervalMillis(),
                TimeUnit.MILLISECONDS
        );
    }
}
