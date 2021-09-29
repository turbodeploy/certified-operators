package com.vmturbo.components.common.featureflags;

import java.util.Properties;

/**
 * Feature flag store that obtains feature enablement state from a {@link Properties} object.
 */
public class PropertiesFeatureFlagEnablementStore implements FeatureFlagEnablementStore {

    private final Properties properties;

    /**
     * Create a new instance.
     * @param properties {@link Properties} object specifying enablement property values
     */
    public PropertiesFeatureFlagEnablementStore(final Properties properties) {
        this.properties = properties;
    }

    @Override
    public boolean isEnabled(final FeatureFlag featureFlag) {
        final String propValue = properties.getProperty(
                FeatureFlagEnablementStoreBase.getConfigPropertyName(featureFlag));
        return propValue != null ? Boolean.valueOf(propValue) : false;
    }
}
