package com.vmturbo.mediation.client.it;

import com.vmturbo.mediation.common.integration.tests.ActionApprovalTest;
import com.vmturbo.mediation.common.tests.util.ISdkEngine;

/**
 * Action approval requests test for XL.
 */
public class XlActionApprovalTest extends ActionApprovalTest {
    @Override
    protected ISdkEngine createSdkEngine() throws Exception {
        return new XlSdkEngine(getThreadPool(), tmpFolder, testName);
    }
}
