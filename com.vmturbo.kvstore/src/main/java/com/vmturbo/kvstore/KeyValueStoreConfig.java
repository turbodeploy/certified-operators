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

    // temporary fix to base the upgrade kv store root on the component_type, since
    // instance_id will change from invocation to invocation, especially version to version
    // TODO: migrate kv store root for existing customers to something more reasonable and remove
    // the "-1"
    @Value("${component_type}-1")
    private String applicationName;

    @Value("${kvStoreTimeoutSeconds:120}")
    private long kvStoreTimeoutSeconds;

    @Value("${consulNamespace:}")
    private String consulNamespace;

    @Value("${enableConsulNamespace:false}")
    private boolean enableConsulNamespace;

    @Bean
    public KeyValueStore keyValueStore() {
        return new ConsulKeyValueStore(
                ConsulKeyValueStore.constructNamespacePrefix(consulNamespace, enableConsulNamespace),
                applicationName,
                consulHost,
                consulPort,
                kvStoreTimeoutSeconds,
                TimeUnit.SECONDS
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

    protected long getKvStoreTimeoutSecond() {
        return kvStoreTimeoutSeconds;
    }
}
