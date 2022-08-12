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
            FeatureFlags.ENABLE_TP_PROBE_SECURITY);

    @Override
    protected ISdkEngine createSdkEngine() throws Exception {
        return new XlSdkEngine(getThreadPool(), tmpFolder, testName);
    }
}
