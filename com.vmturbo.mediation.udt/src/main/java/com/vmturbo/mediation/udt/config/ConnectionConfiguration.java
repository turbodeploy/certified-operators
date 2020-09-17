package com.vmturbo.mediation.udt.config;

import javax.annotation.ParametersAreNonnullByDefault;

import com.vmturbo.topology.processor.api.impl.TopologyProcessorClientConfig;

/**
 * Configuration class that hosts TopologyProcessor configuration and connection properties.
 */
public class ConnectionConfiguration {

    private final ConnectionProperties properties;
    private final TopologyProcessorClientConfig tpConfig;

    /**
     * Constructor.
     *
     * @param properties - connections properties.
     * @param tpConfig   - TopologyProcessor configuration.
     */
    @ParametersAreNonnullByDefault
    public ConnectionConfiguration(ConnectionProperties properties, TopologyProcessorClientConfig tpConfig) {
        this.properties = properties;
        this.tpConfig = tpConfig;
    }

    public ConnectionProperties getProperties() {
        return properties;
    }

    public TopologyProcessorClientConfig getTpConfig() {
        return tpConfig;
    }
}
