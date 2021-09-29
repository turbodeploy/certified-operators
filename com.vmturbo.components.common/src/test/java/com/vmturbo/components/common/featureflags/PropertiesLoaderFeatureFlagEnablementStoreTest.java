package com.vmturbo.components.common.featureflags;

import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;

import org.junit.Before;
import org.junit.Test;
import org.springframework.mock.env.MockEnvironment;

import com.vmturbo.components.common.BaseVmtComponent.ContextConfigurationException;

/**
 * Tests of the {@link PropertiesLoaderFeatureFlagEnablementStore} class.
 */
public class PropertiesLoaderFeatureFlagEnablementStoreTest {

    private static final MockEnvironment env = new MockEnvironment();
    private final PropertiesLoaderFeatureFlagEnablementStore store = new PropertiesLoaderFeatureFlagEnablementStore(env);

    /**
     * Create a new instance.
     *
     * @throws ContextConfigurationException required by {@link PropertiesLoaderFeatureFlagEnablementStoreTest}
     *                                       constructor.
     */
    public PropertiesLoaderFeatureFlagEnablementStoreTest() throws ContextConfigurationException {
    }

    /**
     * Register our store before every test.
     */
    @Before
    public void before() {
        FeatureFlagManager.setStore(store, true);
    }

    /**
     * Ensure that basic enablement checks work as expected.
     */
    @Test
    public void testThatEnablementWorks() {
        final FeatureFlag ff = new FeatureFlag("FF", "ff");
        assertThat(ff.isEnabled(), is(false));
        enableFeature(ff);
        assertThat(ff.isEnabled(), is(true));
    }

    /**
     * Utility to enable a feature in the mock store.
     *
     * @param feature feature to be enabled
     */
    private void enableFeature(FeatureFlag feature) {
        env.setProperty(FeatureFlagEnablementStoreBase.getConfigPropertyName(feature), "true");
    }
}
