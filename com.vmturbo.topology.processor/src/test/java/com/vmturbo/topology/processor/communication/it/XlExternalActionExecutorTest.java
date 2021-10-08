package com.vmturbo.topology.processor.communication.it;

import com.vmturbo.mediation.common.integration.tests.ExternalActionExecutorTest;
import com.vmturbo.mediation.common.tests.util.ISdkEngine;

/**
 * SDK action execution test for XL.
 */
public class XlExternalActionExecutorTest extends ExternalActionExecutorTest {
    @Override
    protected ISdkEngine createSdkEngine() throws Exception {
        return new XlSdkEngine(getThreadPool(), tmpFolder, testName);
    }
}
