package com.vmturbo.components.common.featureflags;

import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;

import java.util.Properties;

import org.junit.Before;
import org.junit.Test;

/**
 * Tests of the {@link PropertiesFeatureFlagEnablementStore} class.
 */
public class PropertiesFeatureFlagEnablementStoreTest {

    private static final Properties properties = new Properties();
    private final PropertiesFeatureFlagEnablementStore store
            = new PropertiesFeatureFlagEnablementStore(properties);

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
     * Utility to enable a feature in properties object.
     *
     * @param feature feature to be enabled
     */
    private void enableFeature(FeatureFlag feature) {
        properties.setProperty(FeatureFlagEnablementStoreBase.getConfigPropertyName(feature), "true");
    }
}
