package com.vmturbo.mediation.udt.config;

import javax.annotation.Nonnull;

import com.vmturbo.platform.sdk.probe.properties.IProbePropertySpec;
import com.vmturbo.platform.sdk.probe.properties.IPropertyProvider;
import com.vmturbo.platform.sdk.probe.properties.PropertySpec;

/**
 * Configuration properties of the probe.
 */
public class UdtProbeConfiguration {

    private static final IProbePropertySpec<Integer> POOL_SIZE =
            new PropertySpec<>("pool.size", Integer::valueOf, -1);

    private final IPropertyProvider propertyProvider;

    /**
     * Constructor.
     *
     * @param propertyProvider - provider of configuration properties.
     */
    public UdtProbeConfiguration(@Nonnull IPropertyProvider propertyProvider) {
        this.propertyProvider = propertyProvider;
    }

    /**
     * Returns a count of threads for an executor service.
     *
     * @return a count of threads
     */
    public int getPoolSize() {
        return propertyProvider.getProperty(POOL_SIZE);
    }
}
