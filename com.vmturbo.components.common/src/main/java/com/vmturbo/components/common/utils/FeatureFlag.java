package com.vmturbo.components.common.utils;

import com.google.common.annotations.VisibleForTesting;

/**
 * Class that defines all feature flags.
 *
 * <p>This class is separated out as much as possible from the underlying implementation of feature
 * flags in order to isolate the actual definitions of feature flag constants from the
 * implementation logic, while still permitting the feature flags themselves to be public static
 * members of {@link FeatureFlag} class.</p>
 */
public class FeatureFlag extends FeatureFlagImpl {

    // Note: change visibility of RETIRED and CANCELED feature flags to private in order to find
    // and remove all mention elsewhere in the code base, while leaving the feature flag for
    // historical and management purposes.

    /** Reporting in SaaS, with data provided by Data Cloud. */
    public static final FeatureFlag SAAS_REPORTING = new FeatureFlag("SaaS Reporting", "saasReporting");


    /**
     * Create a new instance with a given name and enablement property name.
     *
     * @param name               feature flag name
     * @param enablementProperty boolean property that conveys this flag's enablement
     */
    @VisibleForTesting
    FeatureFlag(String name, String enablementProperty) {
        super(name, enablementProperty);
    }

    /**
     * Dummy feature flag that can be used to "fault in" all the real definitions by ensuring that
     * the {@link FeatureFlag} class is fully initialized.
     */
    @VisibleForTesting
    static final FeatureFlag DUMMY = new FeatureFlag("Dummy", "dummy");
}
