package com.vmturbo.components.common.featureflags;

import com.google.common.annotations.VisibleForTesting;

import org.springframework.core.env.Environment;
import org.springframework.web.context.support.AnnotationConfigWebApplicationContext;

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
    PropertiesLoaderFeatureFlagEnablementStore(Environment environment) throws ContextConfigurationException {
        if (environment == null) {
            final AnnotationConfigWebApplicationContext context =
                    new AnnotationConfigWebApplicationContext();
            PropertiesLoader.addConfigurationPropertySources(context);
            environment = context.getEnvironment();
        }
        this.environment = environment;
    }

    /**
     * Create a new instance that draws enablement state from properties injected by {@link
     * PropertiesLoader}.
     *
     * @throws ContextConfigurationException if there's a problem loading properties
     */
    public PropertiesLoaderFeatureFlagEnablementStore() throws ContextConfigurationException {
        this(null);
    }

    @Override
    public boolean isEnabled(final FeatureFlag feature) {
        final String property = FeatureFlagEnablementStoreBase.getConfigPropertyName(feature);
        return environment.getProperty(property, Boolean.class, false);
    }
}
