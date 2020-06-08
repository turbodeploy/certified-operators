package com.vmturbo.mediation.client.it;

import com.vmturbo.mediation.common.integration.tests.ActionAuditTest;
import com.vmturbo.mediation.common.tests.util.ISdkEngine;

/**
 * Action audit test for XL.
 */
public class XlActionAuditTest extends ActionAuditTest {
    @Override
    protected ISdkEngine createSdkEngine() throws Exception {
        return new XlSdkEngine(getThreadPool(), tmpFolder, testName);
    }
}
