package com.vmturbo.components.common.featureflags;

import com.google.common.annotations.VisibleForTesting;

import org.springframework.core.env.Environment;

import com.vmturbo.components.common.BaseVmtComponent.ContextConfigurationException;
import com.vmturbo.components.common.config.PropertiesLoader;

/**
 * {@link FeatureFlagEnablementStore} implementation that obtains enablement state from the CR properties
 * present in a kubernetes deployment of Turbonomic.
 */
public class PropertiesLoaderFeatureFlagEnablementStore implements FeatureFlagEnablementStore {

    private final Environment environment;

    /**
     * Create a new instance that will draw enablement state from the provided environment.
     *
     * <p>This is intended for use only in tests of this store class.</p>
     *
     * @param environment environment to be used for feature enablement data, or null to construct
     *                    one using {@link PropertiesLoader}.
     * @throws ContextConfigurationException if there's a problem loading properties
     */
    @VisibleForTesting
    public PropertiesLoaderFeatureFlagEnablementStore(Environment environment) {
        this.environment = environment;
    }

    @Override
    public boolean isEnabled(final FeatureFlag feature) {
        final String property = FeatureFlagEnablementStoreBase.getConfigPropertyName(feature);
        return environment.getProperty(property, Boolean.class, false);
    }
}
