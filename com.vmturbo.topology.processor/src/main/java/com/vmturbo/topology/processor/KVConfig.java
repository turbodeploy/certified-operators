package com.vmturbo.topology.processor;

import java.util.concurrent.TimeUnit;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import com.vmturbo.kvstore.ConsulKeyValueStore;
import com.vmturbo.kvstore.KeyValueStore;
import com.vmturbo.kvstore.KeyValueStoreConfig;

/**
 * Configuration for the {@link KeyValueStore} used by the
 * topology processor.
 */
@Configuration
public class KVConfig extends KeyValueStoreConfig {

    @Value("${consulNamespace:}")
    private String consulNamespace;

    @Value("${enableConsulNamespace:false}")
    private boolean enableConsulNamespace;

    @Bean
    public KeyValueStore keyValueStoreTelemetry() {
        return new ConsulKeyValueStore(
                ConsulKeyValueStore.constructNamespacePrefix(consulNamespace, enableConsulNamespace),
                "telemetry",
                getConsulHost(),
                getConsulPort(),
                getKvStoreTimeoutSecond(),
                TimeUnit.SECONDS
        );
    }
}
