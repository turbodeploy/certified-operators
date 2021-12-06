package com.vmturbo.components.common.featureflags;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import javax.annotation.Nonnull;

import com.google.common.annotations.VisibleForTesting;

import org.springframework.core.env.Environment;

/**
 * {@link FeatureFlagEnablementStore} implementation that obtains enablement state from the CR properties
 * present in a kubernetes deployment of Turbonomic.
 */
public class PropertiesLoaderFeatureFlagEnablementStore implements FeatureFlagEnablementStore {

    private final Environment environment;
    /**
     * Cache the enablement status to speed up the lookups.
     * Use a {@link ConcurrentHashMap} to prevent concurrent updates by multiple threads.
     */
    private final Map<FeatureFlag, Boolean> enablementCache = new ConcurrentHashMap<>();

    /**
     * Create a new instance that will draw enablement state from the provided environment.
     *
     * @param environment environment to be used for feature enablement data.
     */
    @VisibleForTesting
    public PropertiesLoaderFeatureFlagEnablementStore(@Nonnull Environment environment) {
        this.environment = environment;
    }

    @Override
    public boolean isEnabled(final FeatureFlag feature) {
        return enablementCache.computeIfAbsent(feature, f -> {
            final String property = FeatureFlagEnablementStoreBase.getConfigPropertyName(f);
            return environment.getProperty(property, Boolean.class, false);
        });
    }
}
