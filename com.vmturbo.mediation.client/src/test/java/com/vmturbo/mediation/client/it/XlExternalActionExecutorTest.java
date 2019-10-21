package com.vmturbo.mediation.client.it;

import com.vmturbo.components.common.ConsulRegistrationConfig;
import com.vmturbo.mediation.common.integration.tests.ExternalActionExecutorTest;
import com.vmturbo.mediation.common.tests.util.ISdkEngine;

/**
 * SDK action execution test for XL.
 */
public class XlExternalActionExecutorTest extends ExternalActionExecutorTest {
    @Override
    protected ISdkEngine createSdkEngine() throws Exception {
        System.setProperty(ConsulRegistrationConfig.ENABLE_CONSUL_REGISTRATION, "false");
        return new XlSdkEngine(getThreadPool(), tmpFolder, testName);
    }
}
