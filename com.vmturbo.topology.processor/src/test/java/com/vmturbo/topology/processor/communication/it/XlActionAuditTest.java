package com.vmturbo.topology.processor.communication.it;

import org.junit.Rule;

import com.vmturbo.components.common.featureflags.FeatureFlags;
import com.vmturbo.mediation.common.integration.tests.ActionAuditTest;
import com.vmturbo.mediation.common.tests.util.ISdkEngine;
import com.vmturbo.test.utils.FeatureFlagTestRule;

/**
 * Action audit test for XL.
 */
public class XlActionAuditTest extends ActionAuditTest {

    /**
     * Feature flag rule.
     */
    @Rule
    public FeatureFlagTestRule featureFlagTestRule = new FeatureFlagTestRule(
            FeatureFlags.ENABLE_PROBE_AUTH);
    /**
     * Feature flag rule mandatory.
     */
    @Rule
    public FeatureFlagTestRule featureFlagTestRuleMandatory = new FeatureFlagTestRule(
            FeatureFlags.ENABLE_MANDATORY_PROBE_AUTH);

    @Override
    protected ISdkEngine createSdkEngine() throws Exception {
        return new XlSdkEngine(getThreadPool(), tmpFolder, testName);
    }
}
